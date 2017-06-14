package sgload

import (
	"fmt"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type UpdaterSpec struct {
	NumUpdatesPerDocRequired int           // The number of updates this updater is supposed to do for each doc.
	NumUniqueDocsPerUpdater  int           // The number of unique docs this updater is tasked to update.
	BatchSize                int           // How many docs to update per bulk_docs request
	RevsPerUpdate            int           // How many revisions to include in each document update
	DocSizeBytes             int           // The doc size in bytes to use when generating update docs
	DelayBetweenUpdates      time.Duration // Delay between updates (subtracting out the time they are blocked during write)
}

type Updater struct {
	Agent
	UpdaterSpec

	DocsToUpdate <-chan []DocumentMetadata // This is a channel that this updater listens to for docs that are ready to be updated

	DocUpdateStatuses map[string]DocUpdateStatus // The number of updates and latest rev that have been done per doc id.  Key = doc id, value = number of updates and latest rev

}

type DocUpdateStatus struct {
	NumUpdates       int
	DocumentMetadata DocumentMetadata
}

func NewUpdater(agentSpec AgentSpec, numUniqueDocsPerUpdater, numUpdatesPerDoc, batchsize, docSizeBytes int, revsPerUpdate int, docsToUpdate <-chan []DocumentMetadata, delayBetweenUpdates time.Duration) *Updater {

	updater := &Updater{
		Agent: Agent{
			AgentSpec: agentSpec,
		},
		UpdaterSpec: UpdaterSpec{
			NumUpdatesPerDocRequired: numUpdatesPerDoc,
			BatchSize:                batchsize,
			RevsPerUpdate:            revsPerUpdate,
			NumUniqueDocsPerUpdater:  numUniqueDocsPerUpdater,
			DocSizeBytes:             docSizeBytes,
			DelayBetweenUpdates:      delayBetweenUpdates,
		},
		DocsToUpdate:      docsToUpdate,
		DocUpdateStatuses: map[string]DocUpdateStatus{},
	}

	logger.Info(
		"Updater created",
		"NumUniqueDocsPerUpdater",
		numUniqueDocsPerUpdater,
		"NumUpdatesPerDocRequired",
		numUpdatesPerDoc,
		"RevsPerUpdate",
		revsPerUpdate,
	)

	updater.setupExpVarStats(updatersProgressStats)
	totalUpdatesExpected := int64(numUniqueDocsPerUpdater * updater.NumUpdatesPerDocRequired)
	updater.ExpVarStats.Add(
		"TotalUpdatesExpected",
		totalUpdatesExpected,
	)
	globalProgressStats.Add("TotalNumRevUpdatesExpected", totalUpdatesExpected)

	return updater

}

func (u *Updater) Run() {

	defer u.FinishedWg.Done()

	u.createSGUserIfNeeded([]string{"*"})

	for {
		if u.noMoreExpectedDocsToUpdate() {
			logger.Info(
				"Updater finished",
				"agent.ID",
				u.ID,
				"numdocs",
				u.NumUniqueDocsPerUpdater,
			)
			return
		}

		if len(u.DocUpdateStatuses) < u.NumUniqueDocsPerUpdater {

			logger.Debug("Updater check for more docs to update", "updater", u.UserCred.Username, "numDocUpdateStatuses", len(u.DocUpdateStatuses))

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
						NumUpdates:       0,
						DocumentMetadata: docToUpdate,
					}

					if len(u.DocUpdateStatuses) >= u.NumUniqueDocsPerUpdater {

						logger.Debug("Updater has enough docs to update", "updater", u.UserCred.Username, "numDocUpdateStatuses", len(u.DocUpdateStatuses))

						break

					}

				}
			case <-time.After(time.Second * 10):
				numExpectedUpdatesPending := u.numExpectedUpdatesPending(false)
				logger.Debug(
					"Updater didn't receive anything after 10s",
					"updater",
					u.UserCred.Username,
					"numExpectedUpdatesPending",
					numExpectedUpdatesPending,
				)
			}

		}

		// Grab a batch of docs that need to be updated
		docBatch := u.getDocsReadyToUpdate()

		if len(docBatch) == 0 && u.noMoreExpectedDocsToUpdate() {
			logger.Info(
				"Updater finished",
				"agent.ID",
				u.ID,
				"numdocs",
				u.NumUniqueDocsPerUpdater,
			)
			return
		}

		// If nothing in doc batch, skip this loop iteration
		if len(docBatch) == 0 {
			numExpectedUpdatesPending := u.numExpectedUpdatesPending(false)
			logger.Debug(
				"Updater empty docBatch, call continue",
				"updater",
				u.UserCred.Username,
				"numExpectedUpdatesPending",
				numExpectedUpdatesPending,
			)
			continue
		}

		// Push the update
		timeBeforeUpdate := time.Now()
		logger.Debug("Updater performUpdate", "agent.ID", u.ID, "docbatch", len(docBatch))
		docRevPairsUpdated, err := u.performUpdate(docBatch)
		if err != nil {
			panic(fmt.Sprintf("Error performing update: %v", err))
		}
		timeBlockedDuringUpdate := time.Since(timeBeforeUpdate)

		u.updateDocStatuses(docRevPairsUpdated)

		u.updateExpVars(docRevPairsUpdated)

		numExpectedUpdatesPending := u.numExpectedUpdatesPending(false)

		logger.Debug(
			"Updater pushed changes",
			"updater",
			u.Agent.UserCred.Username,
			"numDocRevPairsUpdated",
			len(docRevPairsUpdated),
			"numExpectedUpdatesPending",
			numExpectedUpdatesPending,
		)

		u.maybeDelayBetweenUpdates(timeBlockedDuringUpdate)

	}

}

func (u Updater) numExpectedUpdatesPending(debug bool) int {

	// We know all of the doc id's we're supposed to be updating, as well
	// as how many updates we expect to do per doc id.  This method finds the
	// "delta" of how many more expected updates are pending overall.

	counter := 0

	// Update the counter to account for the docs that aren't even in DocUpdateStatuses yet,
	// and still need to updated NumUpdatesPerDocRequired times
	numDocsNotYetSeen := u.NumUniqueDocsPerUpdater - len(u.DocUpdateStatuses)

	counter += (numDocsNotYetSeen * u.NumUpdatesPerDocRequired)

	if debug {
		logger.Debug(
			"numExpectedUpdatesPending()",
			"updater",
			u.UserCred.Username,
			"numDocsNotYetSeen",
			numDocsNotYetSeen,
			"NumUpdatesPerDocRequired",
			u.NumUpdatesPerDocRequired,
			"counter",
			counter,
		)
	}

	// Update the counter for remaining revs of each doc that has been seen
	for docId, docStatus := range u.DocUpdateStatuses {
		// Find the delta of how many updates are required compared
		// to how many updates we've made so far
		delta := u.NumUpdatesPerDocRequired - docStatus.NumUpdates
		counter += delta
		if debug {
			logger.Debug(
				"numExpectedUpdatesPending()",
				"updater",
				u.UserCred.Username,
				"docId",
				docId,
				"delta",
				delta,
				"NumUpdates",
				docStatus.NumUpdates,
				"counter",
				counter,
			)
		}

	}
	return counter

}

func (u Updater) noMoreExpectedDocsToUpdate() bool {

	// Find how many pending updates are still remaining
	numExpectedUpdatesPending := u.numExpectedUpdatesPending(false)

	// If no more pending updates remain, we're done
	return numExpectedUpdatesPending == 0

}

// The given docrevpairs were just updated *one* rev.  We need to update
// the doc statuses
func (u *Updater) updateDocStatuses(docRevPairsUpdated []DocumentMetadata) {
	for _, docRevPair := range docRevPairsUpdated {
		docStatus, ok := u.DocUpdateStatuses[docRevPair.Id]
		if !ok {
			panic(fmt.Sprintf("Could not find doc status: %+v", docRevPair))
		}
		docStatus.NumUpdates += 1
		docStatus.DocumentMetadata = docRevPair
		u.DocUpdateStatuses[docRevPair.Id] = docStatus
	}

}

func (u *Updater) updateExpVars(docRevPairsUpdated []DocumentMetadata) {
	u.ExpVarStats.Add("NumDocRevUpdates", int64(len(docRevPairsUpdated)))
	globalProgressStats.Add("TotalNumRevsUpdated", int64(len(docRevPairsUpdated)))
}

func (u *Updater) getDocsReadyToUpdate() []DocumentMetadata {

	return getDocsReadyToUpdate(
		u.BatchSize,
		u.NumUpdatesPerDocRequired,
		u.DocUpdateStatuses,
	)

}

func getDocsReadyToUpdate(batchSize, maxUpdatesPerDoc int, s map[string]DocUpdateStatus) []DocumentMetadata {

	updateDocBatch := []DocumentMetadata{}

	// loop over all docs in DocUpdateStatuses
	for _, docUpdateStatus := range s {

		// if we have enough in batch, return
		if len(updateDocBatch) >= batchSize {
			return updateDocBatch
		}

		// if doc needs more updates, add it to batch
		if docUpdateStatus.NumUpdates < maxUpdatesPerDoc {
			updateDocBatch = append(updateDocBatch, docUpdateStatus.DocumentMetadata)
		}
	}
	return updateDocBatch

}

func (u *Updater) performUpdate(docRevPairs []DocumentMetadata) ([]DocumentMetadata, error) {

	bulkDocs := []Document{}
	for _, docRevPair := range docRevPairs {

		// Copy the document into a new document
		doc := u.generateDocUpdate(docRevPair)

		// TODO: make sure not to skip any generations!  this could mess up accounting

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

	var updatedDocs []DocumentMetadata
	var updatedDoc DocumentMetadata
	var err error

	switch len(bulkDocs) {
	case 1:
		doc := bulkDocs[0]
		updatedDoc, err = u.DataStore.CreateDocument(doc, u.AttachSizeBytes, false)
		updatedDocs = []DocumentMetadata{ updatedDoc }
	default:
		updatedDocs, err = u.DataStore.BulkCreateDocumentsRetry(bulkDocs, false)
	}

	return updatedDocs, err
}

func (u Updater) LookupCurrentRevisions(docsToLookup []Document) ([]sgreplicate.DocumentRevisionPair, error) {

	docRevPairs := []sgreplicate.DocumentRevisionPair{}
	bulkGetRequest := sgreplicate.BulkGetRequest{}
	bulkGetRequestDocs := []sgreplicate.DocumentRevisionPair{}
	for _, docToLookup := range docsToLookup {
		logger.Debug("LookupCurrentRevisions", "docToLookup", fmt.Sprintf("%+v", docToLookup.Id()))
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

func (u *Updater) generateDocUpdate(docRevPair DocumentMetadata) Document {
	doc := map[string]interface{}{}
	doc["_id"] = docRevPair.Id
	doc["bodysize"] = u.DocSizeBytes
	doc["updated_at"] = time.Now().Format(time.RFC3339Nano)
	doc["created_at"] = time.Now().Format(time.RFC3339Nano) // misleading, but not sure what else to do at this point

	doc["channels"] = docRevPair.Channels

	return Document(doc)
}

func (u *Updater) maybeDelayBetweenUpdates(timeBlockedDuringUpdate time.Duration) {

	timeToSleep := u.UpdaterSpec.DelayBetweenUpdates - timeBlockedDuringUpdate
	if timeToSleep > time.Duration(0) {
		time.Sleep(timeToSleep)
	}

}
