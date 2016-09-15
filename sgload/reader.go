package sgload

import "log"

type Reader struct {
	Agent
	SGChannels []string // The Sync Gateway channels this reader is assigned to pull from
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

func (r *Reader) Run() {

	if r.CreateDataStoreUser == true {

		if err := r.DataStore.CreateUser(r.UserCred, r.SGChannels); err != nil {
			log.Fatalf("Error creating user in datastore.  User: %v, Err: %v", r.UserCred, err)
		}
	}

	log.Printf("Run() called")
}
