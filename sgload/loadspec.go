package sgload

// This is the specification for this load test scenario.  The values contained
// here are common to all load test scenarios.
type LoadSpec struct {
	SyncGatewayUrl       string // The Sync Gateway public URL with port and DB, eg "http://localhost:4984/db"
	SyncGatewayAdminPort int    // The Sync Gateway admin port, eg, 4985
	MockDataStore        bool   // If true, will use a MockDataStore instead of a real sync gateway
	StatsdEnabled        bool   // If true, will push stats to StatsdEndpoint
	StatsdEndpoint       string // The endpoint of the statds server, eg localhost:8125
	TestSessionID        string // A unique identifier for this test session.  It's used for creating channel names and possibly more
}

func (ls LoadSpec) Validate() error {
	return nil
}
