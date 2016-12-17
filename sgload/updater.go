package sgload

import (
	"fmt"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type UpdaterSpec struct {
	NumUpdatesPerDocRequired int        // The number of updates this updater is supposed to do for each doc.
	BatchSize                int        // How many docs to update per bulk_docs request
	RevsPerUpdate            int        // How many revisions to include in each document update
	DocsAssignedToUpdater    []Document // The full list of documents that this updater is responsible for updating
}

type Updater struct {
	Agent
	UpdaterSpec

	DocsToUpdate chan []sgreplicate.DocumentRevisionPair // This is a channel that this updater listens to for docs that are ready to be updated

	DocUpdateStatuses map[string]DocUpdateStatus // The number of updates and latest rev that have been done per doc id.  Key = doc id, value = number of updates and latest rev

}

type DocUpdateStatus struct {
	NumUpdates int
	LatestRev  string
}

func NewUpdater(agentSpec AgentSpec, numUpdates int, da []Document, batchsize int, revsPerUpdate int) *Updater {

	docsToUpdate := make(chan []sgreplicate.DocumentRevisionPair, 100)

	updater := &Updater{
		Agent: Agent{
			AgentSpec: agentSpec,
		},
		UpdaterSpec: UpdaterSpec{
			NumUpdatesPerDocRequired: numUpdates,
			BatchSize:                batchsize,
			RevsPerUpdate:            revsPerUpdate,
			DocsAssignedToUpdater:    da,
		},
		DocsToUpdate:      docsToUpdate,
		DocUpdateStatuses: map[string]DocUpdateStatus{},
	}

	updater.setupExpVarStats(updatersProgressStats)
	updater.ExpVarStats.Add(
		"TotalUpdatesExpected",
		int64(len(da)*updater.NumUpdatesPerDocRequired),
	)

	return updater

}

func (u *Updater) Run() {

	defer u.FinishedWg.Done()

	u.createSGUserIfNeeded([]string{"*"})

	for {

		select {
		case docsToUpdate := <-u.DocsToUpdate:

			logger.Debug("Updater received docs to update", "updater", u.UserCred.Username, "numdocs", len(docsToUpdate))

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
		}

		// Grab a batch of docs that need to be updated
		docBatch := u.getDocsReadyToUpdate()
		if len(docBatch) == 0 && u.noMoreExpectedDocsToUpdate() {
			logger.Info("Updater finished", "agent.ID", u.ID, "numdocs", len(u.DocsAssignedToUpdater))
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

		u.updateExpVars(docRevPairsUpdated)

		logger.Debug(
			"Updater pushed changes",
			"updater",
			u.Agent.UserCred.Username,
			"numDocRevPairsUpdated",
			len(docRevPairsUpdated),
		)

	}

}

func (u Updater) numExpectedUpdatesPending() int {

	// We know all of the doc id's we're supposed to be updating, as well
	// as how many updates we expect to do per doc id.  This method finds the
	// "delta" of how many more expected updates are pending overall.

	counter := 0
	for _, docAssignedToUpdater := range u.DocsAssignedToUpdater {
		docId := docAssignedToUpdater.Id()
		docStatus, ok := u.DocUpdateStatuses[docId]
		if !ok {
			// If we haven't even made any updates to that doc id, then
			// we still need to make NumUpdatesPerDocRequired updates
			counter += u.NumUpdatesPerDocRequired
		} else {
			// Find the delta of how many updates are required compared
			// to how many updates we've made so far
			delta := u.NumUpdatesPerDocRequired - docStatus.NumUpdates
			counter += delta
		}
	}
	return counter

}

func (u Updater) noMoreExpectedDocsToUpdate() bool {

	// Find how many pending updates are still remaining
	numExpectedUpdatesPending := u.numExpectedUpdatesPending()

	// If no more pending updates remain, we're done
	return numExpectedUpdatesPending == 0

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

func (u *Updater) updateExpVars(docRevPairsUpdated []sgreplicate.DocumentRevisionPair) {
	u.ExpVarStats.Add("NumDocRevUpdates", int64(len(docRevPairsUpdated)))
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

	bulkDocs := []Document{}
	for _, docRevPair := range docRevPairs {

		// Get the original doc passed to the updater when created, which
		// has all the fields like channels, body, etc
		sourceDoc := u.findDocAssignedToUpdaterById(docRevPair.Id)

		// Copy the document into a new document
		doc := sourceDoc.Copy()

		// Modify a property so that it's a meaningful update
		doc["modifiedProperty"] = docRevPair.Revision

		// Initialize generation and digest based on the previous revision
		generation, parentDigest := parseRevID(docRevPair.Revision)

		// If RevsPerUpdate > 1, mock up a revision history, representing multiple updates made on the client prior to sync
		digests := []string{parentDigest}
		for i := 0; i < u.RevsPerUpdate-1; i++ {
			fakeDigest := generateFakeDigest(i)
			// Prepend to the digests collection
			digests = append(digests, "")
			copy(digests[1:], digests)
			digests[0] = fakeDigest
			// Update generation count and most recent ancestor for use in final rev id calculation
			generation++
			parentDigest = fakeDigest
		}

		// Generate new rev id
		parentRevId := fmt.Sprintf("%d-%s", generation, parentDigest)
		generation++
		newRevId := createRevID(generation, parentRevId, doc)
		//log.Printf("parentRevId, newRevId: (%s, %s)", parentRevId, newRevId)

		_, currentDigest := parseRevID(newRevId)
		// Prepend currentDigest to the digests collection
		digests = append(digests, "")
		copy(digests[1:], digests)
		digests[0] = currentDigest

		// Set the _revisions and _rev for the new_edits=false update
		doc.SetRevisions(generation, digests)
		doc.SetRevision(newRevId)

		bulkDocs = append(bulkDocs, doc)
	}

	updatedDocs, err := u.DataStore.BulkCreateDocumentsRetry(bulkDocs, false)
	if err != nil {
		return updatedDocs, err
	}

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
	u.DocsToUpdate <- docs
}

func (u Updater) LookupCurrentRevisions(docsToLookup []Document) ([]sgreplicate.DocumentRevisionPair, error) {

	docRevPairs := []sgreplicate.DocumentRevisionPair{}
	bulkGetRequest := sgreplicate.BulkGetRequest{}
	bulkGetRequestDocs := []sgreplicate.DocumentRevisionPair{}
	for _, docToLookup := range docsToLookup {
		logger.Info("LookupCurrentRevisions", "docToLookup", fmt.Sprintf("%+v", docToLookup.Id()))
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
