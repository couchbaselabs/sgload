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

func (m MockDataStore) BulkCreateDocuments(docs []Document, newEdits bool) ([]DocumentMetadata, error) {
	log.Printf("MockDataStore BulkCreateDocuments called with %d docs", len(docs))
	return []DocumentMetadata{}, nil
}

func (m MockDataStore) BulkCreateDocumentsRetry(docs []Document, newEdits bool) ([]DocumentMetadata, error) {
	return m.BulkCreateDocumentsRetry(docs, newEdits)
}

func (m *MockDataStore) SetUserCreds(u UserCred) {
	// ignore these
}

func (m MockDataStore) Changes(sinceVal Sincer, limit int, feedType ChangesFeedType) (changes sgreplicate.Changes, newSinceVal Sincer, err error) {
	return sgreplicate.Changes{}, nil, nil
}

func (m MockDataStore) BulkGetDocuments(r sgreplicate.BulkGetRequest) ([]sgreplicate.Document, error) {
	return nil, nil
}

func (m MockDataStore) CreateDocument(doc Document, attachSizeBytes int, newEdits bool) (DocumentMetadata, error) {
	return DocumentMetadata{}, nil
}

