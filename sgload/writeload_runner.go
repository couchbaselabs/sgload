package sgload

import (
	"bytes"
	"fmt"
	"log"
	"sync"
)

type WriteLoadRunner struct {
	LoadRunner
	WriteLoadSpec WriteLoadSpec
}

func NewWriteLoadRunner(wls WriteLoadSpec) *WriteLoadRunner {

	wls.MustValidate()

	loadRunner := LoadRunner{
		LoadSpec: wls.LoadSpec,
	}
	loadRunner.CreateStatsdClient()

	return &WriteLoadRunner{
		LoadRunner:    loadRunner,
		WriteLoadSpec: wls,
	}
}

func (wlr WriteLoadRunner) Run() error {

	// Create a wait group to see when all the writer goroutines have finished
	var wg sync.WaitGroup

	// Create writer goroutines
	writers, err := wlr.createWriters(&wg)
	if err != nil {
		return err
	}
	for _, writer := range writers {
		go writer.Run()
	}

	// Create doc feeder goroutine
	go wlr.feedDocsToWriters(writers)

	// Wait for writers to finish
	log.Printf("Waiting for %d writers to finish...", len(writers))
	wg.Wait()
	log.Printf("Writers finished")

	return nil

}

func (wlr WriteLoadRunner) createWriters(wg *sync.WaitGroup) ([]*Writer, error) {

	writers := []*Writer{}
	var userCreds []UserCred
	var err error

	switch wlr.WriteLoadSpec.CreateWriters {
	case true:
		userCreds = wlr.generateUserCreds()
	default:
		userCreds, err = wlr.WriteLoadSpec.loadUserCredsFromArgs()
		if err != nil {
			return writers, err
		}
	}

	for userId := 0; userId < wlr.WriteLoadSpec.NumWriters; userId++ {
		userCred := userCreds[userId]
		dataStore := wlr.createDataStore()
		dataStore.SetUserCreds(userCred)
		writer := NewWriter(
			wg,
			userId,
			userCred,
			dataStore,
			wlr.WriteLoadSpec.BatchSize,
		)
		writer.CreateDataStoreUser = wlr.WriteLoadSpec.CreateWriters
		writers = append(writers, writer)
		wg.Add(1)
	}

	return writers, nil

}

func (wlr WriteLoadRunner) generateUserCreds() []UserCred {
	userCreds := []UserCred{}
	for userId := 0; userId < wlr.WriteLoadSpec.NumWriters; userId++ {
		username := fmt.Sprintf(
			"writeload-user-%d-%s",
			userId,
			wlr.WriteLoadSpec.TestSessionID,
		)
		password := fmt.Sprintf(
			"writeload-passw0rd-%d-%s",
			userId,
			wlr.WriteLoadSpec.TestSessionID,
		)
		userCred := UserCred{
			Username: username,
			Password: password,
		}
		userCreds = append(userCreds, userCred)

	}
	return userCreds

}

func (wlr WriteLoadRunner) feedDocsToWriters(writers []*Writer) error {

	docsToWrite := wlr.createDocsToWrite()
	docsToWrite = wlr.assignDocsToChannels(docsToWrite)

	// Assign docs to writers, this returns a map keyed on writer which points
	// to doc slice for that writer
	docAssignmentMapping := wlr.assignDocsToWriters(docsToWrite, writers)

	// Loop over doc assignment map and tell each writer to push to data store
	for writer, docsToWrite := range docAssignmentMapping {
		writer.AddToDataStore(docsToWrite)
	}

	// Send terminal docs which will shutdown writers after they've
	// processed all the normal docs
	for _, writer := range writers {
		d := Document{}
		d["_terminal"] = true
		writer.AddToDataStore([]Document{d})
	}

	return nil

}

func (wlr WriteLoadRunner) assignDocsToChannels(inputDocs []Document) []Document {

	docs := []Document{}
	channelNames := wlr.generateChannelNames()

	if len(channelNames) > len(inputDocs) {
		log.Panicf("Number of channels must be less than or equal to number of docs")
	}

	for docNum, inputDoc := range inputDocs {
		chanIndex := docNum % len(channelNames)
		channelName := channelNames[chanIndex]
		inputDoc["channels"] = []string{channelName}
		docs = append(docs, inputDoc)
	}

	return docs

}

func (wlr WriteLoadRunner) generateChannelNames() []string {
	channelNames := []string{}
	for i := 0; i < wlr.WriteLoadSpec.NumChannels; i++ {
		channelName := fmt.Sprintf("%d-%s", i, wlr.WriteLoadSpec.TestSessionID)
		channelNames = append(
			channelNames,
			channelName,
		)
	}
	return channelNames
}

func (wlr WriteLoadRunner) createDocsToWrite() []Document {

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < wlr.WriteLoadSpec.NumDocs; docNum++ {
		d = map[string]interface{}{}
		d["docNum"] = docNum
		d["body"] = createBodyContentWithSize(wlr.WriteLoadSpec.DocSizeBytes)
		docs = append(docs, d)
	}
	return docs

}

// Split the docs among the writers with an even distribution
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

func createBodyContentWithSize(docSizeBytes int) string {
	buf := bytes.Buffer{}
	for i := 0; i < docSizeBytes; i++ {
		buf.WriteString("a")
	}
	return buf.String()
}
