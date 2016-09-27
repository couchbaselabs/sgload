package cmd

import "github.com/couchbaselabs/sgload/sgload"

const (
	NUM_READERS_CMD_NAME    = "numreaders"
	NUM_READERS_CMD_DEFAULT = 100
	NUM_READERS_CMD_DESC    = "The number of unique readers that will read documents.  Each reader runs concurrently in it's own goroutine"

	NUM_WRITERS_CMD_NAME    = "numwriters"
	NUM_WRITERS_CMD_DEFAULT = 100
	NUM_WRITERS_CMD_DESC    = "The number of unique users that will write documents.  Each writer runs concurrently in it's own goroutine"

	CREATE_WRITERS_CMD_NAME    = "createwriters"
	CREATE_WRITERS_CMD_DEFAULT = false
	CREATE_WRITERS_CMD_DESC    = "Add this flag if you need the test to create SG users for writers.  Otherwise you'll need to specify writercreds"

	NUM_CHANS_PER_READER_CMD_NAME    = "num-chans-per-reader"
	NUM_CHANS_PER_READER_CMD_DEFAULT = 1
	NUM_CHANS_PER_READER_CMD_DESC    = "The number of channels that each reader has access to."

	CREATE_READERS_CMD_NAME    = "createreaders"
	CREATE_READERS_CMD_DEFAULT = false
	CREATE_READERS_CMD_DESC    = "Add this flag if you need the test to create SG users for readers.  Otherwise you'll need to specify readercreds"

	SKIP_WRITELOAD_CMD_NAME    = "skipwriteload"
	SKIP_WRITELOAD_CMD_DEFAULT = false
	SKIP_WRITELOAD_CMD_DESC    = "By default will first run the corresponding writeload, so that it has documents to read, but set this flag if you've run that step separately"

	NUM_UPDATERS_CMD_NAME    = "numupdaters"
	NUM_UPDATERS_CMD_DEFAULT = 100
	NUM_UPDATERS_CMD_DESC    = "The number of unique users that will update documents.  Each updater runs concurrently in it's own goroutine"

	NUM_REVS_PER_DOC_CMD_NAME    = "numrevsperdoc"
	NUM_REVS_PER_DOC_CMD_DEFAULT = 100
	NUM_REVS_PER_DOC_CMD_DESC    = "The number of revisions per doc to add updates for"
)

func createLoadSpecFromArgs() sgload.LoadSpec {

	loadSpec := sgload.LoadSpec{
		SyncGatewayUrl:       *sgUrl,
		SyncGatewayAdminPort: *sgAdminPort,
		MockDataStore:        *mockDataStore,
		StatsdEnabled:        *statsdEnabled,
		StatsdEndpoint:       *statsdEndpoint,
		TestSessionID:        *testSessionID,
		BatchSize:            *batchSize,
		NumChannels:          *numChannels,
		DocSizeBytes:         *docSizeBytes,
		NumDocs:              *numDocs,
	}
	loadSpec.TestSessionID = sgload.NewUuid()
	loadSpec.DidAutoGenTestSessionID = true
	return loadSpec
}
