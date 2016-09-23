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

	return &GateLoadRunner{
		LoadRunner:      loadRunner,
		WriteLoadRunner: writeLoadRunner,
		ReadLoadRunner:  readLoadRunner,
		GateLoadSpec:    gls,
		PushedDocs:      make(chan []sgreplicate.DocumentRevisionPair),
	}

}

func (glr GateLoadRunner) Run() error {

	// TODO -------------------

	// TODO: 1) writers need to add timestamp in doc of when they wrote the doc
	// TODO: 2) readers need to calculate RT latency delta and push to statsd
	// TODO: 3) instead of finishing when writers finish, block until readers have read all docs written)

	// Start Writers
	writerWaitGroup, writers, err := glr.startWriters()
	if err != nil {
		return err
	}

	// Start Readers
	if err := glr.startReaders(); err != nil {
		return err
	}

	// Start Doc Feeder
	channelNames := glr.generateChannelNames()
	docsToChannelsAndWriters := createAndAssignDocs(
		writers,
		channelNames,
		glr.WriteLoadSpec.NumDocs,
		glr.WriteLoadSpec.DocSizeBytes,
	)
	if err := glr.startDocFeeder(writers); err != nil {
		return err
	}

	// Start updaters
	if err := glr.startUpdaters(len(writers), docsToChannelsAndWriters); err != nil {
		return err
	}

	// Wait until writers finish
	if err := glr.waitUntilWritersFinish(writerWaitGroup); err != nil {
		return err
	}

	// Wait until updaters finish
	// Close glr.PushedDocs channel

	return nil
}

func (glr GateLoadRunner) startUpdaters(numWriters int, docsToChannelsAndWriters map[*Writer][]Document) error {

	// create numwriter updaters, so 1:1 ratio between writer
	// and updater.  pass it a list of docs to get assigned to updating.

	// start updater goroutines

	// Start docUpdaterRouter that reads off of glr.PushedDocs chan
	go func() {
		for pushedDocRevPairs := range glr.PushedDocs {
			logger.Info("DocUpdaterRouter received", "PushedDocs", pushedDocRevPairs)
			// TODO: route it to appropriate updater
			// Look up updater from docsToChannelsAndUpdaters
			// Push to it's channel

		}
	}()

	return nil

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
		go writer.Run()
	}

	return &wg, writers, nil
}

func (glr GateLoadRunner) startReaders() error {

	// Create a wait group that is currently ignored
	var wg sync.WaitGroup

	// Create reader goroutines
	readers, err := glr.createReaders(&wg)
	if err != nil {
		return fmt.Errorf("Error creating readers: %v", err)
	}
	for _, reader := range readers {
		go reader.Run()
	}

	return nil
}

func (glr GateLoadRunner) startDocFeeder(writers []*Writer) error {
	// Create doc feeder goroutine
	// go glr.feedDocsToWriters(writers)
	return nil
}

func (glr GateLoadRunner) waitUntilWritersFinish(writerWaitGroup *sync.WaitGroup) error {
	writerWaitGroup.Wait()
	return nil
}
