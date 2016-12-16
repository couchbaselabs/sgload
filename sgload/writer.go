package sgload

import (
	"fmt"
	"time"

	"github.com/couchbaselabs/sg-replicate"
)

type Writer struct {
	Agent
	OutboundDocs        chan []Document                         // The Docfeeder pushes outbound docs to the writer
	PushedDocs          chan []sgreplicate.DocumentRevisionPair // After docs are sent, push to this channel
	ExpectedDocsWritten []Document
}

func NewWriter(agentSpec AgentSpec) *Writer {

	outboundDocs := make(chan []Document)

	writer := &Writer{
		Agent: Agent{
			AgentSpec: agentSpec,
		},
		OutboundDocs: outboundDocs,
	}

	writer.setupExpVarStats(writersProgressStats)

	return writer
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
			w.ExpVarStats.Add("NumDocsPushed", int64(len(docs)))
			logger.Debug(
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

func (w *Writer) SetExpectedDocsWritten(docs []Document) {
	w.ExpectedDocsWritten = docs
	w.ExpVarStats.Add("TotalDocs", int64(len(docs)))
	logger.Debug("Writer SetExpectedDocsWritten", "totaldocs", len(docs))
}

func (w *Writer) notifyDocsPushed(docs []sgreplicate.DocumentRevisionPair) {

	logger.Debug("Writer notifying doc updater that docs have been written", "writer", w.UserCred.Username, "numdocs", len(docs))
	start := time.Now()
	if w.PushedDocs != nil {
		w.PushedDocs <- docs
	}
	delta := time.Since(start)
	if delta > time.Second {
		logger.Warn("Writer took more than 1s notify updater docs pushed", "writer", w.UserCred.Username, "numdocs", len(docs), "delta", delta)
	}

}

func (w *Writer) AddToDataStore(docs []Document) {

	switch w.BatchSize {
	case 1:
		for _, doc := range docs {
			logger.Debug("Push single doc to writer", "writer", w.UserCred.Username)
			w.OutboundDocs <- []Document{doc}
		}

	default:
		docBatches := breakIntoBatches(w.BatchSize, docs)
		for _, docBatch := range docBatches {
			logger.Debug("Push doc batch to writer", "writer", w.UserCred.Username, "batchsize", len(docBatch))
			w.OutboundDocs <- docBatch
		}
	}

}
