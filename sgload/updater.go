package sgload

import (
	"fmt"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type Updater struct {
	Agent
	DocsAssignedToUpdater    []Document                              // The full list of documents that this updater is responsible for updating
	DocsToUpdate             chan []sgreplicate.DocumentRevisionPair // This is a channel that this updater listens to for docs that are ready to be updated
	NumUpdatesPerDocRequired int                                     // The number of updates this updater is supposed to do for each doc.
	DocUpdateStatuses        map[string]DocUpdateStatus              // The number of updates and latest rev that have been done per doc id.  Key = doc id, value = number of updates and latest rev
	BatchSize                int                                     // How many docs to update in a batch

}

type DocUpdateStatus struct {
	NumUpdates int
	LatestRev  string
}

func NewUpdater(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, n int, da []Document) *Updater {

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
		DocsAssignedToUpdater:    da,
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
		if len(docBatch) == 0 && u.noMoreExpectedDocsToUpdate() {
			logger.Info("Updater finished", "updater", u)
			return
		}

		// If nothing in doc batch, skip this loop iteration
		if len(docBatch) == 0 {
			continue
		}

		// Push the update
		docRevPairsUpdated, err := u.performUpdate(docBatch)
		if err != nil {
			panic(fmt.Sprintf("Error performing update: %v", err))
		}

		u.updateDocStatuses(docRevPairsUpdated)

	}

}

func (u Updater) noMoreExpectedDocsToUpdate() bool {
	// We know all of the doc id's we're supposed to be updating.
	// If those are all represented in DocUpdateStatuses and
	// they are all maxed out, then we're done
	for _, docAssignedToUpdater := range u.DocsAssignedToUpdater {
		docId := docAssignedToUpdater.Id()
		docStatus, ok := u.DocUpdateStatuses[docId]
		if !ok {
			// didn't find this doc that was assigned to this updater, so
			// therefore we are expecting more docs to update
			return false
		}
		if docStatus.NumUpdates < u.NumUpdatesPerDocRequired {
			// more updates required on this doc
			return false
		}
	}

	// Iterated through all docs assigned to us, and we've updated them
	// all to the num updates required of us.  Therefore, no more doc updates
	// expected/required of us
	return true

}

// The given docrevpairs were just updated *one* rev.  We need to update
// the doc statuses
func (u *Updater) updateDocStatuses(docRevPairsUpdated []sgreplicate.DocumentRevisionPair) {
	for _, docRevPair := range docRevPairsUpdated {
		docStatus, ok := u.DocUpdateStatuses[docRevPair.Id]
		if !ok {
			panic(fmt.Sprintf("Could not find doc status: %+v", docRevPair))
		}
		docStatus.NumUpdates += 1
		docStatus.LatestRev = docRevPair.Revision
		u.DocUpdateStatuses[docRevPair.Id] = docStatus
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

func (u *Updater) performUpdate(docRevPairs []sgreplicate.DocumentRevisionPair) ([]sgreplicate.DocumentRevisionPair, error) {

	logger.Info("Updater.performUpdate", "numdocs", len(docRevPairs), "docs", docRevPairs)

	bulkDocs := []Document{}
	for _, docRevPair := range docRevPairs {

		// Get the original doc passed to the updater when created, which
		// has all the fields like channels, body, etc
		sourceDoc := u.findDocAssignedToUpdaterById(docRevPair.Id)

		// Copy the document into a new document
		doc := sourceDoc.Copy()

		// Update the revision the latest known revision
		doc.SetRevision(docRevPair.Revision)

		// Add something onto the body so that it's a meaningful update
		doc["body"] = fmt.Sprintf("%s-%s", doc["body"], docRevPair.Revision)

		bulkDocs = append(bulkDocs, doc)
	}

	updatedDocs, err := u.DataStore.BulkCreateDocuments(bulkDocs)
	if err != nil {
		return updatedDocs, err
	}

	logger.Info("performUpdateOK", "updatedDocs", updatedDocs)

	return updatedDocs, nil
}

// Lookup doc in DocsAssignedToUpdater slice by ID by iterating
// over the entire slice.
// TODO: use a map
func (u *Updater) findDocAssignedToUpdaterById(docId string) Document {
	for _, doc := range u.DocsAssignedToUpdater {
		if doc.Id() == docId {
			return doc
		}
	}
	panic(fmt.Sprintf("Could not find doc by id: %v", docId))
}

// Tell this updater that the following docs (which presumably are in its list of
// docs that it's responsible for updating) have been inserted into Sync Gateway
func (u *Updater) NotifyDocsReadyToUpdate(docs []sgreplicate.DocumentRevisionPair) {

	logger.Info("NotifyDocsReadyToUpdate", "docs", docs)
	u.DocsToUpdate <- docs
	logger.Info("/NotifyDocsReadyToUpdate")

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
