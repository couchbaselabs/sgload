package sgload

import "log"

type Reader struct {
	Agent
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

func (w *Reader) Run() {
	log.Printf("Run() called")
}
