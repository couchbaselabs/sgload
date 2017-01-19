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
	PushedDocs   chan []DocumentMetadata
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
		PushedDocs:       make(chan []DocumentMetadata, pushedDocsBufferedChannelSize),
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

	// Create a wait group so agents can wait until all SG users
	// have been created (better simulates gateload behavior)
	waitForAllSGUsersCreated := glr.CreateAllSGUsersWaitGroup()

	// Start Writers
	logger.Info("Starting writers")
	writerWaitGroup, writers, err := glr.startWriters(
		waitForAllSGUsersCreated,
		glr.pushToUpdaters(),
	)
	if err != nil {
		return err
	}

	// Start Readers
	logger.Info("Starting readers")
	readerWaitGroup, err := glr.startReaders(waitForAllSGUsersCreated)
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
		glr.UpdateLoadSpec.NumDocs,
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

func (glr GateLoadRunner) pushToUpdaters() bool {
	return (glr.UpdateLoadSpec.NumUpdaters > 0)
}

func (glr GateLoadRunner) CreateAllSGUsersWaitGroup() *sync.WaitGroup {
	numAgents := glr.WriteLoadSpec.NumWriters + glr.ReadLoadSpec.NumReaders
	wg := &sync.WaitGroup{}
	wg.Add(numAgents)
	return wg
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

func (glr GateLoadRunner) startUpdaters(agentCreds []UserCred, numUniqueDocsToUpdate int) (*sync.WaitGroup, []*Updater, error) {

	// Create a wait group to see when all the updater goroutines have finished
	var wg sync.WaitGroup

	// Create updater goroutines
	updaters, err := glr.createUpdaters(&wg, agentCreds, numUniqueDocsToUpdate, glr.PushedDocs)
	if err != nil {
		return nil, nil, err
	}
	for _, updater := range updaters {
		go updater.Run()
	}

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

func (glr GateLoadRunner) startWriters(waitForAllSGUsersCreated *sync.WaitGroup, pushToUpdaters bool) (*sync.WaitGroup, []*Writer, error) {

	// Create a wait group to see when all the writer goroutines have finished
	wg := sync.WaitGroup{}

	globalProgressStats.Add("TotalNumWriterUsers", int64(glr.WriteLoadSpec.NumWriters))
	globalProgressStats.Add("NumWriterUsers", 0)


	// Create writer goroutines
	writers, err := glr.createWriters(&wg, waitForAllSGUsersCreated)
	if err != nil {
		return nil, nil, err
	}
	for _, writer := range writers {

		// This is the main wiring between the writers and the updaters.
		// Whenever a writers writes a doc, it pushes the doc/rev pair to
		// this channel which gives the updater the green light to
		// start updating it.
		if pushToUpdaters {
			writer.PushedDocs = glr.PushedDocs
		}
		go writer.Run()
	}

	return &wg, writers, nil
}

func (glr GateLoadRunner) startReaders(waitForAllSGUsersCreated *sync.WaitGroup) (*sync.WaitGroup, error) {

	wg := sync.WaitGroup{}

	globalProgressStats.Add("TotalNumReaderUsers", int64(glr.ReadLoadSpec.NumReaders))
	globalProgressStats.Add("NumReaderUsers", 0)

	// Create reader goroutines
	readers, err := glr.createReaders(&wg, waitForAllSGUsersCreated)
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
