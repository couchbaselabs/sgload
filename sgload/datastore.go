package sgload

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type DataStore interface {

	// Creates a new user in the data store (admin port)
	CreateUser(u UserCred, channelNames []string) error

	// Creates a document in the data store
	CreateDocument(d Document) (sgreplicate.DocumentRevisionPair, error)

	// Bulk creates a set of documents in the data store
	BulkCreateDocuments(d []Document, newEdits bool) ([]sgreplicate.DocumentRevisionPair, error)

	// Sets the user credentials to use for all subsequent requests
	SetUserCreds(u UserCred)

	// Get all the changes since the since value
	Changes(sinceVal Sincer, limit int) (changes sgreplicate.Changes, newSinceVal Sincer, err error)

	// Does a bulk get on docs in bulk get request, discards actual docs
	BulkGetDocuments(sgreplicate.BulkGetRequest) ([]sgreplicate.Document, error)
}

type UserCred struct {
	Username string `json:"username"` // Username part of basicauth credentials for this writer to use
	Password string `json:"password"` // Password part of basicauth credentials for this writer to use
}

func (u UserCred) Empty() bool {
	return u.Username == "" && u.Password == ""
}

type Document map[string]interface{}

func (d Document) Id() string {
	return d["_id"].(string)
}

func (d Document) Revision() string {
	return d["_rev"].(string)
}

func (d Document) SetRevision(revision string) {
	d["_rev"] = revision
}

// Standard CouchDB encoding of a revision list: digests without numeric generation prefixes go in
// the "ids" property, and the first (largest) generation number in the "start" property.
func (d Document) SetRevisions(start int, digests []string) {
	revisions := Document{}
	revisions["start"] = start
	revisions["ids"] = digests
	d["_revisions"] = revisions
}

func (d Document) Copy() Document {
	doc := Document{}
	for k, v := range d {
		doc[k] = v
	}
	return doc
}

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

func (d Document) channelNames() []string {
	channelNames := []string{}
	channelNamesIface, ok := d["channels"]
	if !ok {
		return channelNames
	}
	channelNamesStr := channelNamesIface.([]string)
	for _, chanName := range channelNamesStr {
		channelNames = append(channelNames, chanName)
	}
	return channelNames
}

func docsMustBeInExpectedChannels(docs []sgreplicate.Document, expectedChannels []string) {

	for _, doc := range docs {
		channels := doc.Body.ChannelNames()
		for _, channel := range channels {
			if !containedIn(channel, expectedChannels) {
				panic(
					fmt.Sprintf("Doc %v has channel %v which is not in expected channels: %v",
						docs,
						channel,
						expectedChannels,
					),
				)
			}
		}
	}

}

func containedIn(s string, expectedIn []string) bool {

	for _, expectedItem := range expectedIn {
		if s == expectedItem {
			return true
		}
	}

	return false

}

func createRevID(generation int, parentRevID string, body Document) string {
	digester := md5.New()
	digester.Write([]byte{byte(len(parentRevID))})
	digester.Write([]byte(parentRevID))
	digester.Write(canonicalEncoding(stripSpecialProperties(body)))
	return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}

func stripSpecialProperties(body Document) Document {
	stripped := Document{}
	for key, value := range body {
		if key == "" || key[0] != '_' || key == "_attachments" || key == "_deleted" {
			stripped[key] = value
		}
	}
	return stripped
}

func canonicalEncoding(body Document) []byte {
	encoded, err := json.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("Couldn't encode body %v", body))
	}
	return encoded
}

// Splits a revision ID into generation number and hex digest.
func parseRevID(revid string) (int, string) {
	if revid == "" {
		return 0, ""
	}
	var generation int
	var id string
	n, _ := fmt.Sscanf(revid, "%d-%s", &generation, &id)
	if n < 1 || generation < 1 {
		return -1, ""
	}
	return generation, id
}

func generateFakeDigest(i int) string {
	digester := md5.New()
	digester.Write([]byte(fmt.Sprintf("%d", i)))
	return fmt.Sprintf("%x", digester.Sum(nil))
}
