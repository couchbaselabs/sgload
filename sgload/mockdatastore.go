package sgload

import "log"

type MockDataStore struct{}

func NewMockDataStore() *MockDataStore {
	return &MockDataStore{}
}

func (m MockDataStore) CreateUser(u UserCred) error {
	log.Printf("MockDataStore CreateUser called with %+v", u)
	return nil
}

func (m MockDataStore) CreateDocument(d Document) error {
	log.Printf("MockDataStore CreateDocument called with %+v", d)
	return nil
}

func (m *MockDataStore) SetUserCreds(u UserCred) {
	// ignore these
}
