package sgload

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	logger.Info("Waiting for writers to finish", "numwriters", len(writers))
	wg.Wait()
	logger.Info("Writers finished")

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
		userCreds, err = wlr.loadUserCredsFromArgs()
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

// TODO: duplicated code with ReadLoadRunner.loadUserCredsFromArgs()
func (wlr WriteLoadRunner) loadUserCredsFromArgs() ([]UserCred, error) {

	userCreds := []UserCred{}
	var err error

	switch {
	case wlr.WriteLoadSpec.WriterCreds != "":
		logger.Info("Load writer creds from CLI args")
		err = json.Unmarshal([]byte(wlr.WriteLoadSpec.WriterCreds), &userCreds)
		if err != nil {
			return userCreds, err
		}
		for _, userCred := range userCreds {
			if userCred.Empty() {
				return userCreds, fmt.Errorf("User credentials empty: %+v", userCred)
			}
		}
	case wlr.WriteLoadSpec.TestSessionID != "" && wlr.WriteLoadSpec.DidAutoGenTestSessionID == false:
		// If the user explicitly provided a test session ID, then use that
		// to generate user credentials to use.  Presumably these credentials
		// were created before in previous runs.  Doesn't make sense to use
		// this with auto-generated test session ID's, since there is no way
		// that the Sync Gateway will have those users created from prev. runs
		logger.Info("Generate user creds from test session id")
		userCreds = wlr.generateUserCreds()
	default:
		return userCreds, fmt.Errorf("You need to either create writers, specify a test session ID, or specify writer user credentials.  See CLI help.")

	}

	if len(userCreds) != wlr.WriteLoadSpec.NumWriters {
		return userCreds, fmt.Errorf("You only provided %d user credentials, but specified %d writers", len(userCreds), wlr.WriteLoadSpec.NumWriters)
	}

	return userCreds, err
}

func (wlr WriteLoadRunner) generateUserCreds() []UserCred {
	return wlr.LoadRunner.generateUserCreds(wlr.WriteLoadSpec.NumWriters, "writeload")
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
		panic(fmt.Sprintf("Num chans (%d) must be LTE to num docs (%d)", len(channelNames), len(inputDocs)))
	}

	for docNum, inputDoc := range inputDocs {
		chanIndex := docNum % len(channelNames)
		channelName := channelNames[chanIndex]
		inputDoc["channels"] = []string{channelName}
		docs = append(docs, inputDoc)
	}

	return docs

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
