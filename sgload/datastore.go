package sgload

import sgreplicate "github.com/couchbaselabs/sg-replicate"

type DataStore interface {

	// Creates a new user in the data store (admin port)
	CreateUser(u UserCred, channelNames []string) error

	// Creates a document in the data store
	CreateDocument(d Document) error

	// Bulk creates a set of documents in the data store
	BulkCreateDocuments(d []Document) error

	// Sets the user credentials to use for all subsequent requests
	SetUserCreds(u UserCred)

	// Get all the changes since the since value
	Changes(sinceVal Sincer, limit int) (changes sgreplicate.Changes, newSinceVal Sincer, err error)

	// Does a bulk get on docs in bulk get request, discards actual docs
	BulkGetDocuments(sgreplicate.BulkGetRequest) error
}

type UserCred struct {
	Username string `json:"username"` // Username part of basicauth credentials for this writer to use
	Password string `json:"password"` // Password part of basicauth credentials for this writer to use
}

func (u UserCred) Empty() bool {
	return u.Username == "" && u.Password == ""
}

type Document map[string]interface{}

type Change interface{} // TODO: spec this out further

type BulkDocs struct {
	NewEdits  bool       `json:"new_edits"`
	Documents []Document `json:"docs"`
}

// Copy-pasted from sg-replicate -- needs to be refactored into common code per DRY principle
type DocumentRevisionPair struct {
	Id       string `json:"id"`
	Revision string `json:"rev"`
	Error    string `json:"error,omitempty"`
	Reason   string `json:"reason,omitempty"`
}
