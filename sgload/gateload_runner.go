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

	// TODO: this might need to have a proper queue rather than a buffered channel.
	// The problem with a buffered channel is that we have to pre-allocate the queue
	// to the high water mark, whereas a queue can grow as needed
	pushedDocsBufferedChannelSize := 10000

	return &GateLoadRunner{
		LoadRunner:       loadRunner,
		WriteLoadRunner:  writeLoadRunner,
		ReadLoadRunner:   readLoadRunner,
		UpdateLoadRunner: updateLoadRunner,
		GateLoadSpec:     gls,
		PushedDocs:       make(chan []sgreplicate.DocumentRevisionPair, pushedDocsBufferedChannelSize),
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
	writerCreds := getWriterCreds(writers)

	// Set docs expected on writers
	// each writer will get approximately total docs / num writers
	approxDocsPerWriter := glr.WriteLoadSpec.NumDocs / len(writers)
	logger.Info("Setting number of docs per writer", "docsPerWriter", approxDocsPerWriter)
	for _, writer := range writers {
		writer.SetApproxExpectedDocsWritten(approxDocsPerWriter)
	}

	// Start doc feeders
	err = glr.startDocFeeders(
		writers,
		glr.WriteLoadSpec,
		approxDocsPerWriter,
		channelNames,
	)
	if err != nil {
		return err
	}

	// Start updaters
	logger.Info("Starting updaters")
	updaterWaitGroup, _, err := glr.startUpdaters(
		writerCreds,
		glr.UpdateLoadSpec.NumUpdaters,
	)
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

func getWriterCreds(writers []*Writer) []UserCred {
	writerCreds := []UserCred{}
	for _, writer := range writers {
		writerCreds = append(writerCreds, writer.UserCred)
	}
	return writerCreds
}

func (glr GateLoadRunner) startUpdaters(agentCreds []UserCred, numUpdaters int) (*sync.WaitGroup, []*Updater, error) {

	// Create a wait group to see when all the updater goroutines have finished
	var wg sync.WaitGroup

	// Create updater goroutines
	updaters, err := glr.createUpdaters(&wg, agentCreds, numUpdaters)
	if err != nil {
		return nil, nil, err
	}
	for _, updater := range updaters {
		go updater.Run()
	}

	// TODO: the updaters need a reference to the glr.PushedDocs channel
	// and they can just directly read from it.

	/*
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
					updaterAgentUsername, err := findAgentAssignedToDoc(docRevPair, docsToChannelsAndWriters)
					if err != nil {
						panic(fmt.Sprintf("Could not find agent for %v", docRevPair))
					}
					updater := findUpdaterByAgentUsername(updaters, updaterAgentUsername)
					updater.NotifyDocsReadyToUpdate([]sgreplicate.DocumentRevisionPair{docRevPair})
				}
			}
		}()
	*/

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

func (glr GateLoadRunner) waitUntilWritersFinish(writerWaitGroup *sync.WaitGroup) error {
	writerWaitGroup.Wait()
	return nil
}
