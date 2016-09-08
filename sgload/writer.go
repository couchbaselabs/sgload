package sgload

import (
	"log"
	"sync"
)

type Writer struct {
	UserCred
	ID                  int       // The numeric ID of the writer (ephemeral, only stored in memory)
	CreateDataStoreUser bool      // Whether this writer must first create a user on the DataStore service, ot just assume it already exists
	DataStore           DataStore // The target data store where docs will be written
	OutboundDocs        chan Document
	WaitGroup           *sync.WaitGroup
}

func NewWriter(wg *sync.WaitGroup, ID int, u UserCred, d DataStore) *Writer {

	outboundDocs := make(chan Document, 100)

	return &Writer{
		UserCred:     u,
		ID:           ID,
		DataStore:    d,
		OutboundDocs: outboundDocs,
		WaitGroup:    wg,
	}
}

func (w *Writer) Run() {

	defer w.WaitGroup.Done()

	if w.CreateDataStoreUser == true {
		if err := w.DataStore.CreateUser(w.UserCred); err != nil {
			log.Fatalf("Error creating user in datastore.  User: %v, Err: %v", w.UserCred, err)
		}
	}

	for {

		select {
		case doc := <-w.OutboundDocs:

			_, ok := doc["_terminal"]
			if ok {
				return
			}

			if err := w.DataStore.CreateDocument(doc); err != nil {
				log.Fatalf("Error creating doc in datastore.  Doc: %v, Err: %v", doc, err)
			}
		}

	}

}

func (w *Writer) AddToDataStore(docs []Document) {
	for _, doc := range docs {
		log.Printf("Writing doc to writer: %v", w)
		w.OutboundDocs <- doc
		log.Printf("/Writing doc to writer: %v", w)
	}

}
