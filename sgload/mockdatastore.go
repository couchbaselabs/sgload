package sgload

import (
	"log"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type MockDataStore struct{}

func NewMockDataStore() *MockDataStore {
	return &MockDataStore{}
}

func (m MockDataStore) CreateUser(u UserCred, channelNames []string) error {
	log.Printf("MockDataStore CreateUser called with %+v", u)
	return nil
}

func (m MockDataStore) CreateDocument(d Document) (sgreplicate.DocumentRevisionPair, error) {
	log.Printf("MockDataStore CreateDocument called with %+v", d)
	return sgreplicate.DocumentRevisionPair{}, nil
}

func (m MockDataStore) BulkCreateDocuments(docs []Document) ([]sgreplicate.DocumentRevisionPair, error) {
	log.Printf("MockDataStore BulkCreateDocuments called with %d docs", len(docs))
	return []sgreplicate.DocumentRevisionPair{}, nil
}

func (m *MockDataStore) SetUserCreds(u UserCred) {
	// ignore these
}

func (m MockDataStore) Changes(sinceVal Sincer, limit int) (changes sgreplicate.Changes, newSinceVal Sincer, err error) {
	return sgreplicate.Changes{}, nil, nil
}

func (m MockDataStore) BulkGetDocuments(r sgreplicate.BulkGetRequest) error {
	return nil
}
