package sgload

import (
	"fmt"
	"time"
)

type WriterSpec struct {

	// How long writers should try to delay between writes
	// (subtracting out the time they are blocked during actual write)
	DelayBetweenWrites time.Duration
}

type Writer struct {
	Agent

	WriterSpec

	OutboundDocs chan []Document           // The Docfeeder pushes outbound docs to the writer
	PushedDocs   chan<- []DocumentMetadata // After docs are sent, push to this channel

}

func NewWriter(agentSpec AgentSpec, spec WriterSpec) *Writer {

	outboundDocs := make(chan []Document)

	writer := &Writer{
		Agent: Agent{
			AgentSpec: agentSpec,
		},
		WriterSpec:   spec,
		OutboundDocs: outboundDocs,
	}

	writer.setupExpVarStats(writersProgressStats)

	return writer
}


func (w *Writer) Run() {

	defer w.FinishedWg.Done()

	numDocsPushed := 0

	w.createWriterSGUserIfNeeded()

	w.waitUntilAllSGUsersCreated()

	for {

		select {
		case docs := <-w.OutboundDocs:

			timeBlockedDuringWrite := time.Duration(0)

			switch len(docs) {
			case 1:
				doc := docs[0]
				_, ok := doc["_terminal"]
				if ok {
					logger.Info("Writer finished", "agent.ID", w.ID, "numdocs", numDocsPushed)
					return
				}

				timeBeforeWrite := time.Now()

				docRevPair, err := w.DataStore.CreateDocument(doc, w.AttachSizeBytes, true)
				if err != nil {
					panic(fmt.Sprintf("Error creating doc in datastore.  Doc: %v, Err: %v", doc, err))
				}
				timeBlockedDuringWrite = time.Since(timeBeforeWrite)
				docRevPairs := []DocumentMetadata{docRevPair}

				w.notifyDocsPushed(docRevPairs)
				numDocsPushed += len(docRevPairs)

			default:
				timeBeforeWrite := time.Now()
				docRevPairs, err := w.DataStore.BulkCreateDocumentsRetry(docs, true)
				if err != nil {
					panic(fmt.Sprintf("Error creating docs in datastore.  Docs: %v, Err: %v", docs, err))
				}
				timeBlockedDuringWrite = time.Since(timeBeforeWrite)
				w.notifyDocsPushed(docRevPairs)
				numDocsPushed += len(docRevPairs)
			}

			w.ExpVarStats.Add("NumDocsPushed", int64(len(docs)))
			globalProgressStats.Add("TotalNumDocsPushed", int64(len(docs)))
			logger.Debug(
				"Writer pushed docs",
				"writer",
				w.Agent.UserCred.Username,
				"numpushed",
				len(docs),
				"totalpushed",
				numDocsPushed,
			)

			if timeBlockedDuringWrite > 10*time.Second {
				logger.Warn("Writer took a while to write", "delta", timeBlockedDuringWrite, "user", w.Agent.UserCred.Username)
			}

			w.maybeDelayBetweenWrites(timeBlockedDuringWrite)

		}

	}

}

func (w *Writer) createWriterSGUserIfNeeded() {
	defer globalProgressStats.Add("NumWriterUsers", 1)
	w.createSGUserIfNeeded([]string{"*"})
}

func updateCreatedAtTimestamp(docs []Document) {
	for _, doc := range docs {
		doc["created_at"] = time.Now().Format(time.RFC3339Nano)
	}
}

func (w *Writer) maybeDelayBetweenWrites(timeBlockedDuringWrite time.Duration) {

	timeToSleep := w.WriterSpec.DelayBetweenWrites - timeBlockedDuringWrite
	if timeToSleep > time.Duration(0) {
		logger.Debug(
			"Writer delay between writes",
			"writer",
			w.Agent.UserCred.Username,
			"delay",
			timeToSleep,
		)
		time.Sleep(timeToSleep)
	}

}

func (w *Writer) SetApproxExpectedDocsWritten(numdocs int) {
	w.ExpVarStats.Add("ApproxTotalDocs", int64(numdocs))
	globalProgressStats.Add("TotalNumDocsPushedExpected", int64(numdocs))
}

func (w *Writer) notifyDocsPushed(docs []DocumentMetadata) {

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
			logger.Debug("Push single doc to writer", "writer", w.UserCred.Username, "channels", doc["channels"])
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
