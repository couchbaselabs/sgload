package sgload

import "log"

type MockDataStore struct {
	MaxConcurrentHttpClients chan struct{} // TODO: currenty ignored
}

func NewMockDataStore(maxConcurrentHttpClients chan struct{}) *MockDataStore {
	return &MockDataStore{
		MaxConcurrentHttpClients: maxConcurrentHttpClients,
	}
}

func (m MockDataStore) CreateUser(u UserCred) error {
	log.Printf("MockDataStore CreateUser called with %+v", u)
}

func (m MockDataStore) CreateDocument(d Document) error {
	log.Printf("MockDataStore CreateDocument called with %+v", d)
}
