package sgload

import "sync"

type GateLoadRunner struct {
	LoadRunner
	WriteLoadRunner
	ReadLoadRunner
	GateLoadSpec GateLoadSpec
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
	if err := glr.startDocFeeder(writers); err != nil {
		return err
	}

	// Wait until writers finish
	if err := glr.waitUntilWritersFinish(writerWaitGroup); err != nil {
		return err
	}

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
	return nil
}

func (glr GateLoadRunner) startDocFeeder(writers []*Writer) error {
	// Create doc feeder goroutine
	go glr.feedDocsToWriters(writers)
	return nil
}

func (glr GateLoadRunner) waitUntilWritersFinish(writerWaitGroup *sync.WaitGroup) error {
	writerWaitGroup.Wait()
	return nil
}
