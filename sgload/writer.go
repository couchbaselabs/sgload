package sgload

import "log"

type Writer struct {
	UserCred
	ID                  int       // The numeric ID of the writer (ephemeral, only stored in memory)
	CreateDataStoreUser bool      // Whether this writer must first create a user on the DataStore service, ot just assume it already exists
	DataStore           DataStore // The target data store where docs will be written
	OutboundDocs        chan Document
}

func NewWriter(ID int, u UserCred, d DataStore) *Writer {

	// TODO: create a channel that will be used by AddToQueue to
	// add actions (DocCreate or other actions)

	outboundDocs := make(chan Document, 100)

	return &Writer{
		UserCred:     u,
		ID:           ID,
		DataStore:    d,
		OutboundDocs: outboundDocs,
	}
}

func (w *Writer) Run() {

	if w.CreateDataStoreUser == true {
		if err := w.DataStore.CreateUser(w.UserCred); err != nil {
			log.Fatalf("Error creating user in datastore.  User: %v, Err: %v", w.UserCred, err)
		}
	}

	for {

		doc := <-w.OutboundDocs
		if err := w.DataStore.CreateDocument(doc); err != nil {
			log.Fatalf("Error creating doc in datastore.  Doc: %v, Err: %v", doc, err)
		}
	}

}

func (w *Writer) AddToDataStore(docs []Document) {
	for _, doc := range docs {
		w.OutboundDocs <- doc
	}

}
