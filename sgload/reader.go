package sgload

import (
	"log"
	"time"
)

type Reader struct {
	Agent
	SGChannels      []string // The Sync Gateway channels this reader is assigned to pull from
	NumDocsExpected int      // The total number of docs this reader is expected to pull
	BatchSize       int      // The number of docs to pull in batch (_changes feed and bulk_get)
}

func NewReader(ID int, u UserCred, d DataStore, batchsize int) *Reader {

	return &Reader{
		Agent: Agent{
			UserCred:  u,
			ID:        ID,
			DataStore: d,
			BatchSize: batchsize,
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

		changes, since, err := r.DataStore.Changes(since, r.BatchSize)

		/*
			{
			  "results":[
			    {
			      "seq":"5:4",
			      "id":"e83a41f812a8265ab6cf72725cfbfc5b",
			      "changes":[
			        {
			          "rev":"1-3391cbe0ca0da56892481ca6f5db6402"
			        }
			      ]
			    },
			    {
			      "seq":5,
			      "id":"_user/readload-user-0-foo",
			      "changes":[

			      ]
			    }
			  ],
			  "last_seq":"5"
			}
		*/

		// Strip out any changes with id "id":"_user/*" since they are user docs and we don't care about them
		// changes = stripUserDocChanges(changes)
		// docRevisionPairs = getDocRevisionPairs(changes)

		// docs := r.DataStore.BulkGetDocuments(docRevisionPairs)

		// numDocsPulled += len(docs)

		log.Printf("changes: %v, since: %v, err: %v", changes, since, err)

		<-time.After(time.Second * 5)

	}

}
