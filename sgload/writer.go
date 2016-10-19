package sgload

import (
	"fmt"
	"sync"
	"time"

	"github.com/couchbaselabs/sg-replicate"
)

type Writer struct {
	Agent
	OutboundDocs chan []Document                         // The Docfeeder pushes outbound docs to the writer
	PushedDocs   chan []sgreplicate.DocumentRevisionPair // After docs are sent, push to this channel
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

	w.createSGUserIfNeeded([]string{"*"})

	for {

		select {
		case docs := <-w.OutboundDocs:

			switch len(docs) {
			case 1:
				doc := docs[0]
				_, ok := doc["_terminal"]
				if ok {
					logger.Info("Writer finished", "agent.ID", w.ID, "numdocs", numDocsPushed)
					return
				}

				docRevPair, err := w.DataStore.CreateDocument(doc)
				if err != nil {
					panic(fmt.Sprintf("Error creating doc in datastore.  Doc: %v, Err: %v", doc, err))
				}
				w.notifyDocPushed(docRevPair)

			default:
				docRevPairs, err := w.DataStore.BulkCreateDocuments(docs, true)
				if err != nil {
					panic(fmt.Sprintf("Error creating docs in datastore.  Docs: %v, Err: %v", docs, err))
				}
				w.notifyDocsPushed(docRevPairs)

			}

			numDocsPushed += len(docs)
			logger.Info(
				"Writer pushed docs",
				"writer",
				w.Agent.UserCred.Username,
				"numpushed",
				numDocsPushed,
			)

		}

	}

}

func updateCreatedAtTimestamp(docs []Document) {
	for _, doc := range docs {
		doc["created_at"] = time.Now().Format(time.RFC3339Nano)
	}
}

func (w *Writer) notifyDocPushed(doc sgreplicate.DocumentRevisionPair) {
	w.notifyDocsPushed([]sgreplicate.DocumentRevisionPair{doc})
}

func (w *Writer) notifyDocsPushed(docs []sgreplicate.DocumentRevisionPair) {
	if w.PushedDocs != nil {
		w.PushedDocs <- docs
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
