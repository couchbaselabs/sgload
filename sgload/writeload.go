package sgload

import (
	"fmt"
	"log"
)

type WriteLoadSpec struct {
	LoadSpec
	NumWriters               int
	NumChannels              int
	DocSizeBytes             int
	MaxConcurrentHttpClients int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
	}
	if err := wls.LoadSpec.Validate(); err != nil {
		return err
	}
	return nil
}

// Validate this spec or panic
func (wls WriteLoadSpec) MustValidate() {
	if err := wls.Validate(); err != nil {
		log.Panicf("Invalid WriteLoadSpec: %+v. Error: %v", wls, err)
	}
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
