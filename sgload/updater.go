package sgload

import (
	"fmt"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type Updater struct {
	Agent
	DocsToUpdate             chan []sgreplicate.DocumentRevisionPair // This is a channel that this updater listens to for docs that are ready to be updated
	NumUpdatesPerDocRequired int                                     // The number of updates this updater is supposed to do for each doc.
	NumUpdatesPerDoc         map[string]int                          // The number of updates that have been done per doc id.  Key = doc id, value = number of updates
	LatestRevPerDoc          map[string]string                       // The latest known revision id for each doc

}

func NewUpdater(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, n int) *Updater {

	docsToUpdate := make(chan []sgreplicate.DocumentRevisionPair, 100)

	return &Updater{
		Agent: Agent{
			FinishedWg: wg,
			UserCred:   u,
			ID:         ID,
			DataStore:  d,
		},
		DocsToUpdate:             docsToUpdate,
		NumUpdatesPerDocRequired: n,
		LatestRevPerDoc:          map[string]string{},
		NumUpdatesPerDoc:         map[string]int{},
	}
}

func (u *Updater) Run() {

	defer u.FinishedWg.Done()

	u.createSGUserIfNeeded([]string{"*"})

	for {

		logger.Info("Updater.Run()", "usercred", u.UserCred.Username)

		select {
		case docsToUpdate := <-u.DocsToUpdate:

			logger.Info("Updater notified docs are ready to update", "DocsToUpdate", docsToUpdate)
			for _, docToUpdate := range docsToUpdate {
				_, ok := u.NumUpdatesPerDoc[docToUpdate.Id]
				if ok {
					// Invalid state, this should be the first
					// time seeing this doc
					panic(fmt.Sprintf("Unexpected doc: %+v", docToUpdate))
				}
				u.NumUpdatesPerDoc[docToUpdate.Id] = 0
				u.LatestRevPerDoc[docToUpdate.Id] = docToUpdate.Revision
			}

		}

		// Grab a batch of docs that need to be updated
		batchSize := 10
		docBatch := u.getDocsReadyToUpdate(batchSize)
		if len(docBatch) == 0 {
			// If all docs have been updated to their max revisions and batch is empty we're done
			logger.Info("Updater finished", "updater", u)
			return
		}

		// Otherwise, update the docs in the batch and update the NumUpdatesPerDoc map
		err := u.performUpdate(docBatch)
		if err != nil {
			panic(fmt.Sprintf("Error performing update: %v", err))
		}

	}

}

func (u *Updater) getDocsReadyToUpdate(batchSize int) []sgreplicate.DocumentRevisionPair {
	return []sgreplicate.DocumentRevisionPair{}
}

func (u *Updater) performUpdate(docs []sgreplicate.DocumentRevisionPair) error {
	logger.Info("Updater.performUpdate", "numdocs", len(docs))
	<-time.After(time.Second * 5)
	return nil
}

// Tell this updater that the following docs (which presumably are in its list of
// docs that it's responsible for updating) have been inserted into Sync Gateway
func (u *Updater) NotifyDocsReadyToUpdate(docs []sgreplicate.DocumentRevisionPair) {

	u.DocsToUpdate <- docs

}

func (u Updater) LookupCurrentRevisions(docsToLookup []Document) ([]sgreplicate.DocumentRevisionPair, error) {

	docRevPairs := []sgreplicate.DocumentRevisionPair{}
	bulkGetRequest := sgreplicate.BulkGetRequest{}
	bulkGetRequestDocs := []sgreplicate.DocumentRevisionPair{}
	for _, docToLookup := range docsToLookup {
		logger.Info("LookupCurrentRevisions", "docToLookup", fmt.Sprintf("%+v", docToLookup))
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = docToLookup["_id"].(string)
		bulkGetRequestDocs = append(bulkGetRequestDocs, docRevPair)
	}
	bulkGetRequest.Docs = bulkGetRequestDocs

	docs, err := u.DataStore.BulkGetDocuments(bulkGetRequest)
	if err != nil {
		return docRevPairs, err
	}

	for _, doc := range docs {
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = doc.Body["_id"].(string)
		docRevPair.Revision = doc.Body["_rev"].(string)
		docRevPairs = append(docRevPairs, docRevPair)
	}

	return docRevPairs, nil

}
