package sgload

import (
	"fmt"
	"sync"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type Updater struct {
	Agent
	DocsToUpdate chan []sgreplicate.DocumentRevisionPair
}

func NewUpdater(wg *sync.WaitGroup, ID int, u UserCred, d DataStore) *Updater {

	docsToUpdate := make(chan []sgreplicate.DocumentRevisionPair, 100)

	return &Updater{
		Agent: Agent{
			FinishedWg: wg,
			UserCred:   u,
			ID:         ID,
			DataStore:  d,
		},
		DocsToUpdate: docsToUpdate,
	}
}

func (u *Updater) Run() {

	defer u.FinishedWg.Done()

	// numDocsPushed := 0

	u.createSGUserIfNeeded([]string{"*"})

	for {

		logger.Info("Updater.Run()", "usercred", u.UserCred.Username)

		select {
		case docsToUpdate := <-u.DocsToUpdate:

			logger.Info("Updater notified docs are ready to update", "DocsToUpdate", docsToUpdate)
		}

	}

}

// Tell this updater that the following docs (which presumably are in its list of
// docs that it's responsible for updating) have been inserted into Sync Gateway
func (u *Updater) NotifyDocsReadyToUpdate(docs []sgreplicate.DocumentRevisionPair) {

	u.DocsToUpdate <- docs

}

func (u Updater) LookupCurrentRevisions(docsToLookup []Document) ([]sgreplicate.DocumentRevisionPair, error) {

	docRevPairs := []sgreplicate.DocumentRevisionPair{}
	bulkGetRequest := sgreplicate.BulkGetRequest{}
	bulkGetRequestDocs := []sgreplicate.DocumentRevisionPair{}
	for _, docToLookup := range docsToLookup {
		logger.Info("LookupCurrentRevisions", "docToLookup", fmt.Sprintf("%+v", docToLookup))
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = docToLookup["_id"].(string)
		bulkGetRequestDocs = append(bulkGetRequestDocs, docRevPair)
	}
	bulkGetRequest.Docs = bulkGetRequestDocs

	docs, err := u.DataStore.BulkGetDocuments(bulkGetRequest)
	if err != nil {
		return docRevPairs, err
	}

	for _, doc := range docs {
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = doc.Body["_id"].(string)
		docRevPair.Revision = doc.Body["_rev"].(string)
		docRevPairs = append(docRevPairs, docRevPair)
	}

	return docRevPairs, nil

}
