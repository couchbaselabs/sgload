package sgload

import (
	"fmt"
	"log"
	"time"
)

type WriteLoadSpec struct {
	LoadSpec
	NumWriters               int
	NumChannels              int
	DocSizeBytes             int
	MaxConcurrentHttpClients int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
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
	WriteLoadSpec.MustValidate()

	// Create a count semaphore so that only MaxConcurrentHttpClients can be active at any given time
	mhcs := make(chan struct{}, wls.MaxConcurrentHttpClients)

	return &WriteLoadRunner{
		WriteLoadSpec:          wls,
		MaxHttpClientSemaphore: mhc,
	}
}

func (wlr WriteLoadRunner) Run() error {

	// Create writers
	writers := wlr.createWriters()
	for _, writer := range writers {
		writer.setMaxHttpClientSemaphore(maxHttpClientSemaphore)
		go writer.Run()
	}

	go wlr.feedDocsToWriters()

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

func (wlr WriteLoadRunner) createWriters() []*Writer {

	writers := []*Writer{}
	userCreds := []UserCred{}

	switch wlr.WriteLoadSpec.CreateUsers {
	case true:
		wlr.WriteLoadSpec.loadUserCreds(userCreds)
	default:
		userCreds = generateUserCreds(userCreds)
	}

	for userId := range wlr.WriteLoadSpec.NumWriters {
		userCred := userCreds[userId]
		writer := NewWriter(userId, userCred, wlr.dataStore())
		writer.CreateDataStoreUser = wlr.WriteLoadSpec.CreateUsers
		writers = append(writers, writer)
	}

	return writers

}

func (wlr WriteLoadRunner) feedDocsToWriters() error {

	docsToWrite := wlr.createDocsToWrite()
	docAssignmentMapping := wlr.assignDocsToWriters(docsToWrite, writers)
	for _, docToWrite := range docsToWrite {
		writer := docAssignmentMapping[docToWrite]
		writer.AddToQueue(docToWrite)
	}

}
