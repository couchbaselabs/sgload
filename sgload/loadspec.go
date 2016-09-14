package sgload

import (
	"fmt"

	"github.com/satori/go.uuid"
)

// This is the specification for this load test scenario.  The values contained
// here are common to all load test scenarios.
type LoadSpec struct {
	SyncGatewayUrl       string // The Sync Gateway public URL with port and DB, eg "http://localhost:4984/db"
	SyncGatewayAdminPort int    // The Sync Gateway admin port, eg, 4985
	MockDataStore        bool   // If true, will use a MockDataStore instead of a real sync gateway
	StatsdEnabled        bool   // If true, will push stats to StatsdEndpoint
	StatsdEndpoint       string // The endpoint of the statds server, eg localhost:8125
	TestSessionID        string // A unique identifier for this test session.  It's used for creating channel names and possibly more
	BatchSize            int    // How many docs to read (bulk_get) or write (bulk_docs) in bulk
	NumChannels          int    // How many channels to create/use during this test
	DocSizeBytes         int    // Doc size in bytes to create during this test
	NumDocs              int    // Number of docs to read/write during this test
}

func (ls LoadSpec) Validate() error {
	return nil
}

func (ls *LoadSpec) GenerateTestSessionID() {
	// If the user didn't pass in a test session id, autogen one
	if ls.TestSessionID == "" {
		ls.TestSessionID = NewUuid()
	}

}

func NewUuid() string {
	u4 := uuid.NewV4()
	return fmt.Sprintf("%s", u4)
}
