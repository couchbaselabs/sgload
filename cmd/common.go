package cmd

import (
	"github.com/couchbaselabs/sgload/sgload"
	"github.com/inconshreveable/log15"
)

const (
	NUM_READERS_CMD_NAME    = "numreaders"
	NUM_READERS_CMD_DEFAULT = 100
	NUM_READERS_CMD_DESC    = "The number of unique readers that will read documents.  Each reader runs concurrently in it's own goroutine"

	NUM_WRITERS_CMD_NAME    = "numwriters"
	NUM_WRITERS_CMD_DEFAULT = 100
	NUM_WRITERS_CMD_DESC    = "The number of unique users that will write documents.  Each writer runs concurrently in it's own goroutine"

	WRITER_DELAY_CMD_NAME    = "writerdelayms"
	WRITER_DELAY_CMD_DEFAULT = 10000
	WRITER_DELAY_CMD_DESC    = "How long writers should wait in between writes.  The time take to do the previous write will be subtracted out of the delay.  If the time taken for previous write is longer than the delay, the writer will not wait"

	CREATE_WRITERS_CMD_NAME    = "createwriters"
	CREATE_WRITERS_CMD_DEFAULT = false
	CREATE_WRITERS_CMD_DESC    = "Add this flag if you need the test to create SG users for writers."

	NUM_CHANS_PER_READER_CMD_NAME    = "num-chans-per-reader"
	NUM_CHANS_PER_READER_CMD_DEFAULT = 1
	NUM_CHANS_PER_READER_CMD_DESC    = "The number of channels that each reader has access to."

	CREATE_READERS_CMD_NAME    = "createreaders"
	CREATE_READERS_CMD_DEFAULT = false
	CREATE_READERS_CMD_DESC    = "Add this flag if you need the test to create SG users for readers."

	SKIP_WRITELOAD_CMD_NAME    = "skipwriteload"
	SKIP_WRITELOAD_CMD_DEFAULT = false
	SKIP_WRITELOAD_CMD_DESC    = "By default will first run the corresponding writeload, so that it has documents to read, but set this flag if you've run that step separately"

	NUM_UPDATERS_CMD_NAME    = "numupdaters"
	NUM_UPDATERS_CMD_DEFAULT = 100
	NUM_UPDATERS_CMD_DESC    = "The number of unique users that will update documents.  Each updater runs concurrently in it's own goroutine"

	FEED_TYPE_CMD_NAME    = "readerfeedtype"
	FEED_TYPE_CMD_DEFAULT = "longpoll"
	FEED_TYPE_CMD_DESC    = "The changes feed type: normal or longpoll"

	NUM_REVS_PER_DOC_CMD_NAME    = "numrevsperdoc"
	NUM_REVS_PER_DOC_CMD_DEFAULT = 5
	NUM_REVS_PER_DOC_CMD_DESC    = "The number of updates per doc (total revs will be numrevsperdoc * numrevsperupdate)"

	NUM_REVS_PER_UPDATE_CMD_NAME    = "numrevsperupdate"
	NUM_REVS_PER_UPDATE_CMD_DEFAULT = 1
	NUM_REVS_PER_UPDATE_CMD_DESC    = "The number of revisions per doc to add in each update"
)

func createLoadSpecFromArgs() sgload.LoadSpec {

	loadSpec := sgload.LoadSpec{
		SyncGatewayUrl:        *sgUrl,
		SyncGatewayAdminPort:  *sgAdminPort,
		MockDataStore:         *mockDataStore,
		StatsdEnabled:         *statsdEnabled,
		StatsdEndpoint:        *statsdEndpoint,
		StatsdPrefix:          *statsdPrefix,
		TestSessionID:         *testSessionID,
		AttachSizeBytes:       *attachSizeBytes,
		BatchSize:             *batchSize,
		NumChannels:           *numChannels,
		DocSizeBytes:          *docSizeBytes,
		NumDocs:               *numDocs,
		CompressionEnabled:    *compressionEnabled,
		ExpvarProgressEnabled: *expvarProgressEnabled,
	}

	switch *logLevelStr {
	case "critical":
		loadSpec.LogLevel = log15.LvlCrit
	case "error":
		loadSpec.LogLevel = log15.LvlError
	case "warn":
		loadSpec.LogLevel = log15.LvlWarn
	case "info":
		loadSpec.LogLevel = log15.LvlInfo
	case "debug":
		loadSpec.LogLevel = log15.LvlDebug
	}

	loadSpec.TestSessionID = sgload.NewUuid()
	return loadSpec
}
