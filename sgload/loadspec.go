package sgload

import (
	"fmt"

	"github.com/inconshreveable/log15"
	"github.com/satori/go.uuid"
)

// This is the specification for this load test scenario.  The values contained
// here are common to all load test scenarios.
type LoadSpec struct {
	SyncGatewayUrl        string    // The Sync Gateway public URL with port and DB, eg "http://localhost:4984/db"
	SyncGatewayAdminPort  int       // The Sync Gateway admin port, eg, 4985
	MockDataStore         bool      // If true, will use a MockDataStore instead of a real sync gateway
	StatsdEnabled         bool      // If true, will push stats to StatsdEndpoint
	StatsdEndpoint        string    // The endpoint of the statds server, eg localhost:8125
	StatsdPrefix          string    // The metrics prefix to use (for example, some hosted statsd services require a token)
	TestSessionID         string    // A unique identifier for this test session.  It's used for creating channel names and possibly more
	AttachSizeBytes       int       // If > 0, and BatchSize == 1, then it will add attachments of this size during doc creates/updates.
	BatchSize             int       // How many docs to read (bulk_get) or write (bulk_docs) in bulk
	NumChannels           int       // How many channels to create/use during this test
	DocSizeBytes          int       // Doc size in bytes to create during this test
	NumDocs               int       // Number of docs to read/write during this test
	CompressionEnabled    bool      // Whether requests and responses should be compressed (when supported)
	ExpvarProgressEnabled bool      // Whether to publish reader/writer/updater progress to expvars (disabled by default to not bloat expvar json)
	LogLevel              log15.Lvl // The log level.  Defaults to LvlWarn

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
	u4, err := uuid.NewV4()
	if err != nil {
		panic(fmt.Sprintf("Error creating new UUID: %v", err))
	}
	return fmt.Sprintf("%s", u4)
}
