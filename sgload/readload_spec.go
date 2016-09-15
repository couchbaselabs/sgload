package sgload

import (
	"encoding/json"
	"fmt"
	"log"
)

type ReadLoadSpec struct {
	LoadSpec
	CreateReaders     bool   // Whether or not to create users for readers
	ReaderCreds       string // The usernames / passwords to use if CreateReaders is set to false
	NumReaders        int
	NumChansPerReader int
	NumDocs           int
}

func (rls ReadLoadSpec) Validate() error {

	if err := rls.LoadSpec.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate this spec or panic
func (rls ReadLoadSpec) MustValidate() {
	if err := rls.Validate(); err != nil {
		log.Panicf("Invalid ReadLoadSpec: %+v. Error: %v", rls, err)
	}
}

func (rls ReadLoadSpec) loadUserCredsFromArgs() ([]UserCred, error) {

	userCreds := []UserCred{}
	err := json.Unmarshal([]byte(rls.ReaderCreds), &userCreds)
	for _, userCred := range userCreds {
		if userCred.Empty() {
			return userCreds, fmt.Errorf("User credentials empty: %+v", userCred)
		}
	}
	return userCreds, err
}
