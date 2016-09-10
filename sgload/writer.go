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
	OutboundDocs        chan []Document
	WaitGroup           *sync.WaitGroup
	BatchSize           int
}

func NewWriter(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, batchsize int) *Writer {

	outboundDocs := make(chan []Document, 100)

	return &Writer{
		UserCred:     u,
		ID:           ID,
		DataStore:    d,
		OutboundDocs: outboundDocs,
		WaitGroup:    wg,
		BatchSize:    batchsize,
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
		case docs := <-w.OutboundDocs:

			switch len(docs) {
			case 1:
				doc := docs[0]
				_, ok := doc["_terminal"]
				if ok {
					return
				}

				if err := w.DataStore.CreateDocument(doc); err != nil {
					log.Fatalf("Error creating doc in datastore.  Doc: %v, Err: %v", doc, err)
				}

			default:
				if err := w.DataStore.BulkCreateDocuments(docs); err != nil {
					log.Fatalf("Error creating docs in datastore.  Docs: %v, Err: %v", docs, err)
				}

			}
		}

	}

}

func (w *Writer) AddToDataStore(docs []Document) {

	switch w.BatchSize {
	case 1:
		for _, doc := range docs {
			w.OutboundDocs <- []Document{doc}
		}

	default:
		docBatches := breakIntoBatches(w.BatchSize, docs)
		for _, docBatch := range docBatches {
			w.OutboundDocs <- docBatch
		}
	}

}

// Break things into batches, for example:
//
// batchSize: 3
// things: [t1, t2, t3, t4, t5]
//
// result:
//
//   [
//     [t1, t2, t3],  <-- batch 1
//     [t4, t5],      <-- batch 2 (incomplete, not enough to fill batch)
//
//   ]
func breakIntoBatches(batchSize int, docs []Document) [][]Document {

	batches := [][]Document{}

	numBatches := len(docs) / batchSize

	// is there residue?  if so, add one more to batch
	if len(docs)%batchSize != 0 {
		numBatches += 1
	}

	for i := 0; i < numBatches; i++ {
		batch := []Document{}
		for j := 0; j < batchSize; j++ {
			docIndex := i*batchSize + j
			if docIndex >= len(docs) {
				break
			}
			doc := docs[docIndex]
			batch = append(batch, doc)
		}
		batches = append(batches, batch)
	}

	return batches

}
