package sgload

type GateLoadRunner struct {
	LoadRunner
	GateLoadSpec GateLoadSpec
}

func NewGateLoadRunner(gls GateLoadSpec) *GateLoadRunner {

	gls.MustValidate()

	loadRunner := LoadRunner{
		LoadSpec: gls.LoadSpec,
	}
	loadRunner.CreateStatsdClient()

	return &GateLoadRunner{
		LoadRunner:   loadRunner,
		GateLoadSpec: gls,
	}

}

func (glr GateLoadRunner) Run() error {

	// TODO -------------------

	// TODO: 1) writers need to add timestamp in doc of when they wrote the doc
	// TODO: 2) readers need to calculate RT latency delta and push to statsd
	// TODO: 3) instead of finishing when writers finish, block until readers have read all docs written)

	// Start Writers
	if err := glr.startWriters(); err != nil {
		return err
	}

	// Start Readers
	if err := glr.startReaders(); err != nil {
		return err
	}

	// Start Doc Feeder
	if err := glr.startDocFeeder(); err != nil {
		return err
	}

	// Wait until writers finish
	if err := glr.waitUntilWritersFinish(); err != nil {
		return err
	}

	return nil
}

func (glr GateLoadRunner) startWriters() error {
	return nil
}

func (glr GateLoadRunner) startReaders() error {
	return nil
}

func (glr GateLoadRunner) startDocFeeder() error {
	return nil
}

func (glr GateLoadRunner) waitUntilWritersFinish() error {
	return nil
}
