package sgload

import (
	"fmt"
	"log"
	"time"
)

type WriteLoadSpec struct {
	LoadSpec

	CreateWriters bool // Whether or not to create users for writers
	NumWriters    int

	// How long writers should try to delay between writes
	// (subtracting out the time they are blocked during actual write)
	DelayBetweenWrites time.Duration
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
