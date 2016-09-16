package sgload

import (
	"log"
	"strings"
	"sync"
	"time"

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
			log.Fatalf("Error creating user in datastore.  User: %v, Err: %v", r.UserCred, err)
		}
	}

	numDocsPulled := 0
	since := StringSincer{}

	for {

		if numDocsPulled > r.NumDocsExpected {
			log.Panicf("Reader was only expected to pull %d docs, but pulled %d.", r.NumDocsExpected, numDocsPulled)
		}

		log.Printf("Reader.Run() numDocsPulled: %d / numDocsExpected: %d", numDocsPulled, r.NumDocsExpected)

		if numDocsPulled == r.NumDocsExpected {
			// reader finished!
			log.Printf("Received all docs -- reader finished")
			return
		}

		changes, newSince, err := r.DataStore.Changes(since, r.BatchSize)
		if err != nil {
			log.Panicf("Got error getting changes: %v", err)
		}
		if newSince.Equals(since) {
			log.Panicf("Since value should have changed from: %v", since)
		}
		since = newSince.(StringSincer)

		// Strip out any changes with id "id":"_user/*" since they are user docs and we don't care about them
		changes = stripUserDocChanges(changes)

		bulkGetRequest := getBulkGetRequest(changes)

		err = r.DataStore.BulkGetDocuments(bulkGetRequest)
		if err != nil {
			log.Panicf("Got error getting bulk docs: %v", err)
		}

		numDocsPulled += len(bulkGetRequest.Docs)

		log.Printf("changes: %+v, since: %v, err: %v", changes, since, err)

		<-time.After(time.Second * 1)

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
