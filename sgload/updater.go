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
	DocUpdateStatuses        map[string]DocUpdateStatus              // The number of updates and latest rev that have been done per doc id.  Key = doc id, value = number of updates and latest rev
	BatchSize                int                                     // How many docs to update in a batch

}

type DocUpdateStatus struct {
	NumUpdates int
	LatestRev  string
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
		DocUpdateStatuses:        map[string]DocUpdateStatus{},
		BatchSize:                5, // TODO: parameterize
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

				_, ok := u.DocUpdateStatuses[docToUpdate.Id]
				if ok {
					// Invalid state, this should be the first
					// time seeing this doc
					panic(fmt.Sprintf("Unexpected doc: %+v", docToUpdate))
				}
				u.DocUpdateStatuses[docToUpdate.Id] = DocUpdateStatus{
					NumUpdates: 0,
					LatestRev:  docToUpdate.Revision,
				}

			}
		case <-time.After(time.Second * 1):
			logger.Info("No more docs in DocsToUpdate")
		}

		// Grab a batch of docs that need to be updated
		docBatch := u.getDocsReadyToUpdate()
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

func (u *Updater) getDocsReadyToUpdate() []sgreplicate.DocumentRevisionPair {

	return getDocsReadyToUpdate(
		u.BatchSize,
		u.NumUpdatesPerDocRequired,
		u.DocUpdateStatuses,
	)

}

func getDocsReadyToUpdate(batchSize, maxUpdatesPerDoc int, s map[string]DocUpdateStatus) []sgreplicate.DocumentRevisionPair {

	updateDocBatch := []sgreplicate.DocumentRevisionPair{}

	// loop over all docs in DocUpdateStatuses
	for docId, docUpdateStatus := range s {

		// if we have enough in batch, return
		if len(updateDocBatch) >= batchSize {
			return updateDocBatch
		}

		// if doc needs more updates, add it to batch
		if docUpdateStatus.NumUpdates < maxUpdatesPerDoc {
			docToUpdate := sgreplicate.DocumentRevisionPair{
				Id:       docId,
				Revision: docUpdateStatus.LatestRev,
			}
			updateDocBatch = append(updateDocBatch, docToUpdate)
		}
	}
	return updateDocBatch

}

func (u *Updater) performUpdate(docRevPairs []sgreplicate.DocumentRevisionPair) error {
	logger.Info("Updater.performUpdate", "numdocs", len(docRevPairs), "docs", docRevPairs)
	// <-time.After(time.Second * 5)

	bulkDocs := []Document{}
	for _, docRevPair := range docRevPairs {
		doc := Document{}
		doc["_id"] = docRevPair.Id
		doc["_rev"] = docRevPair.Revision
		doc["body"] = "updatedbody" // TODO
		bulkDocs = append(bulkDocs, doc)
	}
	updatedDocs, err := u.DataStore.BulkCreateDocuments(bulkDocs)
	if err != nil {
		return err
	}

	logger.Info("performUpdateOK", "updatedDocs", updatedDocs)

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
