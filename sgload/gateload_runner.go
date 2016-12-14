package sgload

import (
	"fmt"
	"sync"

	"github.com/couchbaselabs/sg-replicate"
)

type GateLoadRunner struct {
	LoadRunner
	WriteLoadRunner
	ReadLoadRunner
	UpdateLoadRunner
	GateLoadSpec GateLoadSpec
	PushedDocs   chan []sgreplicate.DocumentRevisionPair
}

func NewGateLoadRunner(gls GateLoadSpec) *GateLoadRunner {

	gls.MustValidate()

	loadRunner := LoadRunner{
		LoadSpec: gls.LoadSpec,
	}
	loadRunner.CreateStatsdClient()

	writeLoadRunner := WriteLoadRunner{
		LoadRunner:    loadRunner,
		WriteLoadSpec: gls.WriteLoadSpec,
	}

	readLoadRunner := ReadLoadRunner{
		LoadRunner:   loadRunner,
		ReadLoadSpec: gls.ReadLoadSpec,
	}

	updateLoadRunner := UpdateLoadRunner{
		LoadRunner:     loadRunner,
		UpdateLoadSpec: gls.UpdateLoadSpec,
	}

	return &GateLoadRunner{
		LoadRunner:       loadRunner,
		WriteLoadRunner:  writeLoadRunner,
		ReadLoadRunner:   readLoadRunner,
		UpdateLoadRunner: updateLoadRunner,
		GateLoadSpec:     gls,
		PushedDocs:       make(chan []sgreplicate.DocumentRevisionPair),
	}

}

func (glr GateLoadRunner) Run() error {

	logger.Info(
		"Running Gateload Scenario",
		"numdocs",
		glr.GateLoadSpec.NumDocs,
		"numwriters",
		glr.GateLoadSpec.NumWriters,
		"numreaders",
		glr.GateLoadSpec.NumReaders,
		"numupdaters",
		glr.GateLoadSpec.NumUpdaters,
		"numchannels",
		glr.GateLoadSpec.NumChannels,
		"numrevsperdoc",
		glr.GateLoadSpec.NumUpdatesPerDoc,
	)

	// Start Writers
	logger.Info("Starting writers")
	writerWaitGroup, writers, err := glr.startWriters()
	if err != nil {
		return err
	}

	// Start Readers
	logger.Info("Starting readers")
	readerWaitGroup, err := glr.startReaders()
	if err != nil {
		return err
	}

	// Start Doc Feeder
	logger.Info("Starting docfeeder")
	channelNames := glr.generateChannelNames()
	writerAgentIds := getWriterAgentIds(writers)

	// if not pre-allocated
	//  - updater needs to know list of doc ids for each updater
	//  - writer needs to know how many docs to expect
	docsToChannelsAndWriters := createAndAssignDocs(
		writerAgentIds,
		channelNames,
		glr.WriteLoadSpec.NumDocs,
		glr.WriteLoadSpec.DocSizeBytes,
		glr.WriteLoadSpec.TestSessionID,
	)
	if err := glr.startDocFeeder(writers, docsToChannelsAndWriters); err != nil {
		return err
	}

	// Set docs expected on writers
	for _, writer := range writers {
		writer.SetExpectedDocsWritten(
			docsToChannelsAndWriters[writer.UserCred.Username],
		)
	}

	// Start updaters
	logger.Info("Starting updaters")
	updaterWaitGroup, _, err := glr.startUpdaters(docsToChannelsAndWriters)
	if err != nil {
		return err
	}

	// Wait until writers finish
	logger.Info("Wait until writers finish")
	if err := glr.waitUntilWritersFinish(writerWaitGroup); err != nil {
		return err
	}
	logger.Info("Writers finished")

	// Wait until readers finish
	logger.Info("Wait until readers finish")
	readerWaitGroup.Wait()
	logger.Info("Readers finished")

	// Wait until updaters finish
	// Close glr.PushedDocs channel
	logger.Info("Wait until updaters finish")
	updaterWaitGroup.Wait()
	logger.Info("Updaters finished")

	return nil
}

func getWriterAgentIds(writers []*Writer) []string {
	writerAgentIds := []string{}
	for _, writer := range writers {
		writerAgentIds = append(writerAgentIds, writer.UserCred.Username)
	}
	return writerAgentIds
}

func (glr GateLoadRunner) startUpdaters(igoredWriterDocs map[string][]Document) (*sync.WaitGroup, []*Updater, error) {

	// Create a wait group to see when all the updater goroutines have finished
	var wg sync.WaitGroup

	// Create usercreds
	userCreds, err := glr.createUserCreds()
	if err != nil {
		return nil, nil, err
	}

	// Generate the mapping between docs+channels and updaters
	channelNames := glr.generateChannelNames()
	updaterAgentUsernames := getUpdaterAgentUsernames(userCreds)

	// If not pre-allocated, would need
	//   - updaters need to have full list of doc ids
	docsToChannelsAndUpdaters := createAndAssignDocs(
		updaterAgentUsernames,
		channelNames,
		glr.UpdateLoadSpec.NumDocs,
		glr.UpdateLoadSpec.DocSizeBytes,
		glr.UpdateLoadSpec.TestSessionID,
	)

	// Create updater goroutines
	updaters, err := glr.createUpdaters(&wg, userCreds, docsToChannelsAndUpdaters)
	if err != nil {
		return nil, nil, err
	}
	for _, updater := range updaters {
		go updater.Run()
	}

	// Start docUpdaterRouter that reads off of glr.PushedDocs chan
	go func() {
		for pushedDocRevPairs := range glr.PushedDocs {

			// If we don't have any updaters consuming notifications
			// about pushed docs, then just ignore them
			if len(updaters) == 0 {
				continue
			}

			for _, docRevPair := range pushedDocRevPairs {
				// route it to appropriate updater
				updaterAgentUsername, err := findAgentAssignedToDoc(docRevPair, docsToChannelsAndUpdaters)
				if err != nil {
					panic(fmt.Sprintf("Could not find agent for %v", docRevPair))
				}
				updater := findUpdaterByAgentUsername(updaters, updaterAgentUsername)
				updater.NotifyDocsReadyToUpdate([]sgreplicate.DocumentRevisionPair{docRevPair})
			}
		}
	}()

	return &wg, updaters, nil

}

func findAgentAssignedToDoc(d sgreplicate.DocumentRevisionPair, docsToChannelsAndAgents map[string][]Document) (string, error) {

	for agentUsername, docs := range docsToChannelsAndAgents {
		for _, doc := range docs {
			if d.Id == doc.Id() {
				return agentUsername, nil
			}

		}
	}
	return "", fmt.Errorf("Could not find agent username with %v in %v", d, docsToChannelsAndAgents)

}

func (glr GateLoadRunner) startWriters() (*sync.WaitGroup, []*Writer, error) {

	// Create a wait group to see when all the writer goroutines have finished
	wg := sync.WaitGroup{}

	// Create writer goroutines
	writers, err := glr.createWriters(&wg)
	if err != nil {
		return nil, nil, err
	}
	for _, writer := range writers {

		// This is the main wiring between the writers and the updaters.
		// Whenever a writers writes a doc, it pushes the doc/rev pair to
		// this channel which gives the updater the green light to
		// start updating it.
		writer.PushedDocs = glr.PushedDocs

		go writer.Run()
	}

	return &wg, writers, nil
}

func (glr GateLoadRunner) startReaders() (*sync.WaitGroup, error) {

	wg := sync.WaitGroup{}

	// Create reader goroutines
	readers, err := glr.createReaders(&wg)
	if err != nil {
		return nil, fmt.Errorf("Error creating readers: %v", err)
	}
	for _, reader := range readers {
		go reader.Run()
	}

	return &wg, nil
}

func (glr GateLoadRunner) startDocFeeder(writers []*Writer, docsToChannelsAndWriters map[string][]Document) error {
	// Create doc feeder goroutine
	go glr.feedDocsToWriters(writers, docsToChannelsAndWriters)
	return nil
}

func (glr GateLoadRunner) waitUntilWritersFinish(writerWaitGroup *sync.WaitGroup) error {
	writerWaitGroup.Wait()
	return nil
}
