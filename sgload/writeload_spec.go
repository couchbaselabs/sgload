package sgload

import (
	"fmt"
	"log"
)

type WriteLoadSpec struct {
	LoadSpec
	CreateWriters bool   // Whether or not to create users for writers
	WriterCreds   string // The usernames / passwords to use if CreateWriters is set to false
	NumWriters    int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
	}

	if len(wls.WriterCreds) > 0 {
		if wls.CreateWriters == true {
			return fmt.Errorf("Cannot only set user credentials if createwriters is set to false")
		}
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
