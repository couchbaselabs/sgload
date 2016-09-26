package sgload

import (
	"sync"

	"github.com/couchbaselabs/sg-replicate"
)

type Updater struct {
	Agent
	InsertedDocs chan []sgreplicate.DocumentRevisionPair
}

func NewUpdater(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, batchsize int) *Updater {

	insertedDocs := make(chan []sgreplicate.DocumentRevisionPair, 100)

	return &Updater{
		Agent: Agent{
			FinishedWg: wg,
			UserCred:   u,
			ID:         ID,
			DataStore:  d,
			BatchSize:  batchsize,
		},
		InsertedDocs: insertedDocs,
	}
}

func (u *Updater) Run() {

	defer u.FinishedWg.Done()

	numDocsPushed := 0

	u.createSGUserIfNeeded([]string{"*"})

	for {

		select {
		case docsInserted := <-u.InsertedDocs:

			logger.Info("Updater notified docs inserted", "DocsInserted", docsInserted)
		}

	}

}

// Tell this updater that the following docs (which presumably are in its list of
// docs that it's responsible for updating) have been inserted into Sync Gateway
func (u *Updater) NotifyDocsInserted(docs []sgreplicate.DocumentRevisionPair) {

	u.InsertedDocs <- []Document{doc}

}
