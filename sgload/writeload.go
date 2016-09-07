package sgload

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type WriteLoadSpec struct {
	LoadSpec
	NumWriters               int
	NumChannels              int
	DocSizeBytes             int
	NumDocs                  int
	MaxConcurrentHttpClients int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
	}

	if !wls.CreateUsers {
		userCreds, err := wls.loadUserCredsFromArgs()
		if err != nil {
			return err
		}
		if len(userCreds) != wls.NumWriters {
			return fmt.Errorf("You only provided %d user credentials, but specified %d writers", len(userCreds), wls.NumWriters)
		}
	}

	if err := wls.LoadSpec.Validate(); err != nil {
		return err
	}
	return nil
}

// Validate this spec or panic
func (wls WriteLoadSpec) MustValidate() {
	if err := wls.Validate(); err != nil {
		log.Panicf("Invalid WriteLoadSpec: %+v. Error: %v", wls, err)
	}
}

type WriteLoadRunner struct {
	WriteLoadSpec          WriteLoadSpec
	MaxHttpClientSemaphore chan struct{}
}

func NewWriteLoadRunner(wls WriteLoadSpec) *WriteLoadRunner {
	wls.MustValidate()

	// Create a count semaphore so that only MaxConcurrentHttpClients can be active at any given time
	mhcs := make(chan struct{}, wls.MaxConcurrentHttpClients)

	return &WriteLoadRunner{
		WriteLoadSpec:          wls,
		MaxHttpClientSemaphore: mhcs,
	}
}

func (wlr WriteLoadRunner) Run() error {

	// Create writers
	writers, err := wlr.createWriters()
	if err != nil {
		return err
	}
	for _, writer := range writers {
		go writer.Run()
	}

	go wlr.feedDocsToWriters(writers)

	// wait until all writers are finished
	// TODO: make this non-lame
	log.Printf("Waiting a few mins")
	<-time.After(time.Second * 120)
	log.Printf("Done waiting -- should be all done by now")

	return nil
}

func (wlr WriteLoadRunner) dataStore() DataStore {

	return NewMockDataStore(wlr.MaxHttpClientSemaphore) // TODO: load data store based on url rather than hardcoding to MockDataStore

}

func (wlr WriteLoadRunner) createWriters() ([]*Writer, error) {

	writers := []*Writer{}
	var userCreds []UserCred
	var err error

	switch wlr.WriteLoadSpec.CreateUsers {
	case true:
		log.Printf("CreateUsers = true, calling generateUserCreds()")
		userCreds = wlr.generateUserCreds()
	default:
		log.Printf("CreateUsers = true, calling loadUserCredsFromArgs()")
		userCreds, err = wlr.WriteLoadSpec.loadUserCredsFromArgs()
		log.Printf("userCreds: %v", userCreds)
		if err != nil {
			return writers, err
		}
	}

	for userId := 0; userId < wlr.WriteLoadSpec.NumWriters; userId++ {
		userCred := userCreds[userId]
		writer := NewWriter(userId, userCred, wlr.dataStore())
		writer.CreateDataStoreUser = wlr.WriteLoadSpec.CreateUsers
		writers = append(writers, writer)
	}

	return writers, nil

}

func (wlr WriteLoadRunner) generateUserCreds() []UserCred {
	userCreds := []UserCred{}
	for userId := 0; userId < wlr.WriteLoadSpec.NumWriters; userId++ {
		userCred := UserCred{
			Username: fmt.Sprintf("writeload-user-%d", userId),
			Password: fmt.Sprintf("writeload-passw0rd-%d", userId),
		}
		userCreds = append(userCreds, userCred)

	}
	return userCreds

}

func (wlr WriteLoadRunner) feedDocsToWriters(writers []*Writer) error {

	docsToWrite := wlr.createDocsToWrite()
	docAssignmentMapping := wlr.assignDocsToWriters(docsToWrite, writers)

	for writer, docsToWrite := range docAssignmentMapping {
		writer.AddToDataStore(docsToWrite)
	}

	return nil

}

func (wlr WriteLoadRunner) createDocsToWrite() []Document {

	// TODO: this needs to distribute docs among the channels
	// TODO: this needs to (approximately) match the doc size

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < wlr.WriteLoadSpec.NumDocs; docNum++ {
		if err := json.Unmarshal([]byte(`{"foo": "bar"}`), &d); err != nil {
			log.Panicf("Could not unmarshal json")
		}
		docs = append(docs, d)
	}
	return docs

}

func (wlr WriteLoadRunner) assignDocsToWriters(d []Document, w []*Writer) map[*Writer][]Document {

	docAssignmentMapping := map[*Writer][]Document{}
	for _, writer := range w {
		docAssignmentMapping[writer] = []Document{}
	}

	for docNum, doc := range d {

		// figure out which writer to assign this doc to
		writerIndex := docNum % len(w)
		writer := w[writerIndex]

		// add doc to writer's list of docs
		docsForWriter := docAssignmentMapping[writer]
		docsForWriter = append(docsForWriter, doc)
		docAssignmentMapping[writer] = docsForWriter

	}

	return docAssignmentMapping

}
