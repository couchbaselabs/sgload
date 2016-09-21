package sgload

import (
	"fmt"
	"sync"
	"time"

	"github.com/peterbourgon/g2s"
)

type Writer struct {
	Agent
	OutboundDocs chan []Document
	WaitGroup    *sync.WaitGroup
}

func NewWriter(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, batchsize int) *Writer {

	outboundDocs := make(chan []Document, 100)

	return &Writer{
		Agent: Agent{
			FinishedWg: wg,
			UserCred:   u,
			ID:         ID,
			DataStore:  d,
			BatchSize:  batchsize,
		},
		OutboundDocs: outboundDocs,
	}
}

func (w *Writer) Run() {

	defer w.FinishedWg.Done()

	numDocsPushed := 0

	w.createSGUserIfNeeded()

	for {

		select {
		case docs := <-w.OutboundDocs:

			updateCreatedAtTimestamp(docs)

			switch len(docs) {
			case 1:
				doc := docs[0]
				_, ok := doc["_terminal"]
				if ok {
					logger.Info("Writer pushed all docs", "agent.ID", w.ID, "numdocs", numDocsPushed)
					return
				}

				if err := w.DataStore.CreateDocument(doc); err != nil {
					panic(fmt.Sprintf("Error creating doc in datastore.  Doc: %v, Err: %v", doc, err))
				}

			default:
				if err := w.DataStore.BulkCreateDocuments(docs); err != nil {
					panic(fmt.Sprintf("Error creating docs in datastore.  Docs: %v, Err: %v", docs, err))
				}

			}

			numDocsPushed += len(docs)
		}

	}

}

func updateCreatedAtTimestamp(docs []Document) {
	for _, doc := range docs {
		doc["created_at"] = time.Now().Format(time.RFC3339Nano)
	}
}

func (w *Writer) SetStatsdClient(statsdClient *g2s.Statsd) {
	w.StatsdClient = statsdClient
}

func (w *Writer) createSGUserIfNeeded() {
	if w.CreateDataStoreUser == true {

		// Just give writers access to all channels
		allChannels := []string{"*"}

		logger.Info("Creating writer SG user", "username", w.UserCred.Username, "channels", allChannels)

		if err := w.DataStore.CreateUser(w.UserCred, allChannels); err != nil {
			panic(fmt.Sprintf("Error creating user in datastore.  User: %v, Err: %v", w.UserCred, err))
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
