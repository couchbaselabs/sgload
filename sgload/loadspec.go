package sgload

import (
	"encoding/json"
	"fmt"
)

// This is the specification for this load test scenario.  The values contained
// here are common to all load test scenarios.
type LoadSpec struct {
	SyncGatewayUrl       string // The Sync Gateway public URL with port and DB, eg "http://localhost:4984/db"
	SyncGatewayAdminPort int    // The Sync Gateway admin port, eg, 4985
	CreateUsers          bool   // Whether or not to create users
	UserCreds            string // The usernames / passwords to use if CreateUsers is set to false
	MockDataStore        bool   // If true, will use a MockDataStore instead of a real sync gateway
	StatsdEnabled        bool   // If true, will push stats to StatsdEndpoint
	StatsdEndpoint       string // The endpoint of the statds server, eg localhost:8125
	TestSessionID        string // A unique identifier for this test session.  It's used for creating channel names and possibly more
}

func (ls LoadSpec) Validate() error {

	// Todo: attempt to connect to SyncGateway URL -- if 404, throw invalid error

	if len(ls.UserCreds) > 0 {
		if ls.CreateUsers == true {
			return fmt.Errorf("Cannot only set user credentials if createusers is set to false")
		}
	}

	return nil
}

func (ls LoadSpec) loadUserCredsFromArgs() ([]UserCred, error) {

	userCreds := []UserCred{}
	err := json.Unmarshal([]byte(ls.UserCreds), &userCreds)
	for _, userCred := range userCreds {
		if userCred.Empty() {
			return userCreds, fmt.Errorf("User credentials empty: %+v", userCred)
		}
	}
	return userCreds, err
}
