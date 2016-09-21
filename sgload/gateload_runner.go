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
	logger.Info("gateload runner", "glr", glr)
	return nil
}
