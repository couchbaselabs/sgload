package sgload

import "log"

type WriteLoadSpec struct {
	LoadSpec
	NumWriters int
}

// Validate this spec or panic
func (wls WriteLoadSpec) MustValidate() {
	// TODO
}

type WriteLoadRunner struct {
	WriteLoadSpec WriteLoadSpec
}

func NewWriteLoadRunner(WriteLoadSpec WriteLoadSpec) *WriteLoadRunner {
	WriteLoadSpec.MustValidate()
	return &WriteLoadRunner{
		WriteLoadSpec: WriteLoadSpec,
	}
}

func (wlr WriteLoadRunner) Run() error {
	log.Printf("Todo: Run() should do something")
	return nil
}
