package sgload

import (
	"fmt"

	"github.com/satori/go.uuid"
)

// This is the specification for this load test scenario.  The values contained
// here are common to all load test scenarios.
type LoadSpec struct {
	SyncGatewayUrl          string // The Sync Gateway public URL with port and DB, eg "http://localhost:4984/db"
	SyncGatewayAdminPort    int    // The Sync Gateway admin port, eg, 4985
	MockDataStore           bool   // If true, will use a MockDataStore instead of a real sync gateway
	StatsdEnabled           bool   // If true, will push stats to StatsdEndpoint
	StatsdEndpoint          string // The endpoint of the statds server, eg localhost:8125
	TestSessionID           string // A unique identifier for this test session.  It's used for creating channel names and possibly more
	DidAutoGenTestSessionID bool   // If the Test Session ID was auto-generated, as opposed to set by the user explicitly, this field will be set to true
	BatchSize               int    // How many docs to read (bulk_get) or write (bulk_docs) in bulk
	NumChannels             int    // How many channels to create/use during this test
	DocSizeBytes            int    // Doc size in bytes to create during this test
	NumDocs                 int    // Number of docs to read/write during this test
}

func (ls LoadSpec) Validate() error {

	if ls.NumChannels > ls.NumDocs {
		return fmt.Errorf("Number of channels must be less than or equal to number of docs")
	}

	if ls.SyncGatewayUrl == "" {
		return fmt.Errorf("%+v missing Sync Gateway URL", ls)
	}
	return nil
}

func (ls *LoadSpec) generateUserCreds(numUsers int, usernamePrefix string) []UserCred {
	userCreds := []UserCred{}
	for userId := 0; userId < numUsers; userId++ {
		username := fmt.Sprintf(
			"%s-user-%d-%s",
			usernamePrefix,
			userId,
			ls.TestSessionID,
		)
		password := fmt.Sprintf(
			"%s-passw0rd-%d-%s",
			usernamePrefix,
			userId,
			ls.TestSessionID,
		)
		userCred := UserCred{
			Username: username,
			Password: password,
		}
		userCreds = append(userCreds, userCred)

	}
	return userCreds
}

func NewUuid() string {
	u4 := uuid.NewV4()
	return fmt.Sprintf("%s", u4)
}
