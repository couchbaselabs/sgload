package sgload

type UserCred struct {
	Username string // Username part of basicauth credentials for this writer to use
	Password string // Password part of basicauth credentials for this writer to use
}

type Writer struct {
	UserCred
	ID                  int       // The numeric ID of the writer (ephemeral, only stored in memory)
	CreateDataStoreUser bool      // Whether this writer must first create a user on the DataStore service, ot just assume it already exists
	DataStore           DataStore // The target data store where docs will be written
}

func NewWriter(ID int, u UserCred, d DataStore) *Writer {

	// TODO: create a channel that will be used by AddToQueue to
	// add actions (DocCreate or other actions)

	return &Writer{
		UserCred:  u,
		ID:        userId,
		DataStore: d,
	}
}

func (w *Writer) Run() {

}

func (w *Writer) AddToQueue() {

}
