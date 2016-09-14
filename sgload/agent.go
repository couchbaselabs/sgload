package sgload

// Contains common fields and functionality between readers and writers
type Agent struct {
	UserCred
	ID                  int       // The numeric ID of the writer (ephemeral, only stored in memory)
	CreateDataStoreUser bool      // Whether this writer must first create a user on the DataStore service, ot just assume it already exists
	DataStore           DataStore // The target data store where docs will be written
	BatchSize           int       // bulk_get or bulk_docs batch size
}
