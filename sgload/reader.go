package sgload

import (
	"fmt"
	"strings"
	"sync"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type Reader struct {
	Agent
	SGChannels      []string // The Sync Gateway channels this reader is assigned to pull from
	NumDocsExpected int      // The total number of docs this reader is expected to pull
	BatchSize       int      // The number of docs to pull in batch (_changes feed and bulk_get)
}

func NewReader(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, batchsize int) *Reader {

	return &Reader{
		Agent: Agent{
			FinishedWg: wg,
			UserCred:   u,
			ID:         ID,
			DataStore:  d,
			BatchSize:  batchsize,
		},
	}
}

func (r *Reader) SetChannels(sgChannels []string) {
	r.SGChannels = sgChannels
}

func (r *Reader) SetNumDocsExpected(n int) {
	r.NumDocsExpected = n
}

func (r *Reader) SetBatchSize(batchSize int) {
	r.BatchSize = batchSize
}

func (r *Reader) Run() {

	defer r.FinishedWg.Done()

	if r.CreateDataStoreUser == true {

		if err := r.DataStore.CreateUser(r.UserCred, r.SGChannels); err != nil {
			panic(fmt.Sprintf("Error creating user in datastore.  User: %v, Err: %v", r.UserCred, err))
		}
	}

	numDocsPulled := 0
	since := StringSincer{}

	for {

		if numDocsPulled > r.NumDocsExpected {
			panic(fmt.Sprintf("Reader was only expected to pull %d docs, but pulled %d.", r.NumDocsExpected, numDocsPulled))
		}

		logger.Info("Reader pulled docs", "agent.ID", r.ID, "numDocsPulled", numDocsPulled, "numDocsExpected", r.NumDocsExpected)

		if numDocsPulled == r.NumDocsExpected {
			// reader finished!
			logger.Info("Reader finished", "agent.ID", r.ID, "numDocsPulled", numDocsPulled, "numDocsExpected", r.NumDocsExpected)
			return
		}

		changes, newSince, err := r.DataStore.Changes(since, r.BatchSize)
		if err != nil {
			panic(fmt.Sprintf("Got error getting changes: %v", err))
		}
		if newSince.Equals(since) {
			panic(fmt.Sprintf("Since value should have changed from: %v", since))
		}
		since = newSince.(StringSincer)

		// Strip out any changes with id "id":"_user/*"
		// since they are user docs and we don't care about them
		changes = stripUserDocChanges(changes)

		bulkGetRequest := getBulkGetRequest(changes)

		err = r.DataStore.BulkGetDocuments(bulkGetRequest)
		if err != nil {
			panic(fmt.Sprintf("Got error getting bulk docs: %v", err))
		}

		numDocsPulled += len(bulkGetRequest.Docs)

	}

}

func getBulkGetRequest(changes sgreplicate.Changes) sgreplicate.BulkGetRequest {

	bulkDocsRequest := sgreplicate.BulkGetRequest{}
	docs := []sgreplicate.DocumentRevisionPair{}
	for _, change := range changes.Results {
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = change.Id
		docRevPair.Revision = change.ChangedRevs[0].Revision
		docs = append(docs, docRevPair)
	}
	bulkDocsRequest.Docs = docs
	return bulkDocsRequest

}

func stripUserDocChanges(changes sgreplicate.Changes) (changesStripped sgreplicate.Changes) {
	changesStripped.LastSequence = changes.LastSequence

	for _, change := range changes.Results {
		if strings.Contains(change.Id, "_user") {
			continue
		}
		changesStripped.Results = append(changesStripped.Results, change)

	}

	return changesStripped
}
