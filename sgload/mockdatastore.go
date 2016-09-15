package sgload

import "log"

type MockDataStore struct{}

func NewMockDataStore() *MockDataStore {
	return &MockDataStore{}
}

func (m MockDataStore) CreateUser(u UserCred, channelNames []string) error {
	log.Printf("MockDataStore CreateUser called with %+v", u)
	return nil
}

func (m MockDataStore) CreateDocument(d Document) error {
	log.Printf("MockDataStore CreateDocument called with %+v", d)
	return nil
}

func (m MockDataStore) BulkCreateDocuments(docs []Document) error {
	log.Printf("MockDataStore BulkCreateDocuments called with %d docs", len(docs))
	return nil
}

func (m *MockDataStore) SetUserCreds(u UserCred) {
	// ignore these
}

func (m MockDataStore) Changes(sinceVal Sincer, limit int) (changes []Change, newSinceVal Sincer, err error) {
	return nil, nil, nil
}
