package sgload

import "log"

type GateLoadSpec struct {
	LoadSpec
	NumReaders int
	NumWriters int
}

func (gls GateLoadSpec) Validate() error {

	if err := gls.LoadSpec.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate this spec or panic
func (gls GateLoadSpec) MustValidate() {
	if err := gls.Validate(); err != nil {
		log.Panicf("Invalid GateLoadSpec: %+v. Error: %v", gls, err)
	}
}
