package sgload

import (
	"encoding/json"
	"fmt"
	"log"
)

type WriteLoadSpec struct {
	LoadSpec
	CreateWriters bool   // Whether or not to create users for writers
	WriterCreds   string // The usernames / passwords to use if CreateWriters is set to false
	NumWriters    int
	NumChannels   int
	DocSizeBytes  int
	NumDocs       int
	BatchSize     int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
	}

	if !wls.CreateWriters {
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

func (wls WriteLoadSpec) loadUserCredsFromArgs() ([]UserCred, error) {

	userCreds := []UserCred{}
	err := json.Unmarshal([]byte(wls.WriterCreds), &userCreds)
	for _, userCred := range userCreds {
		if userCred.Empty() {
			return userCreds, fmt.Errorf("User credentials empty: %+v", userCred)
		}
	}
	return userCreds, err
}
