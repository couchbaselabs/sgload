package sgload

import (
	"bytes"
	"sync"
)

const (
	USER_PREFIX_WRITER = "writer"
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

	channelNames := wlr.generateChannelNames()
	docsToChannelsAndWriters := createAndAssignDocs(
		writers,
		channelNames,
		wlr.WriteLoadSpec.NumDocs,
		wlr.WriteLoadSpec.DocSizeBytes,
	)

	// Create doc feeder goroutine
	go wlr.feedDocsToWriters(writers, docsToChannelsAndWriters)

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
		userCreds, err = wlr.loadUserCredsFromArgs(wlr.WriteLoadSpec.NumWriters, USER_PREFIX_WRITER)
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
		writer.SetStatsdClient(wlr.StatsdClient)
		writer.CreateDataStoreUser = wlr.WriteLoadSpec.CreateWriters
		writers = append(writers, writer)
		wg.Add(1)
	}

	return writers, nil

}

func (wlr WriteLoadRunner) generateUserCreds() []UserCred {
	return wlr.LoadRunner.generateUserCreds(wlr.WriteLoadSpec.NumWriters, USER_PREFIX_WRITER)
}

func (wlr WriteLoadRunner) feedDocsToWriters(writers []*Writer, docsToChannelsAndWriters map[*Writer][]Document) error {

	// Loop over doc assignment map and tell each writer to push to data store
	for writer, docsToWrite := range docsToChannelsAndWriters {
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

func createBodyContentWithSize(docSizeBytes int) string {
	buf := bytes.Buffer{}
	for i := 0; i < docSizeBytes; i++ {
		buf.WriteString("a")
	}
	return buf.String()
}
