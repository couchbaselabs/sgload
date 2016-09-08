package sgload

import (
	"fmt"
	"log"
)

type WriteLoadSpec struct {
	LoadSpec
	NumWriters   int
	NumChannels  int
	DocSizeBytes int
	NumDocs      int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
	}

	if !wls.CreateUsers {
		userCreds, err := wls.loadUserCredsFromArgs()
		if err != nil {
			return err
		}
		if len(userCreds) != wls.NumWriters {
			return fmt.Errorf("You only provided %d user credentials, but specified %d writers", len(userCreds), wls.NumWriters)
		}
	}

	if wls.NumChannels > wls.NumDocs {
		return fmt.Errorf("Number of channels must be less than or equal to number of docs")
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
