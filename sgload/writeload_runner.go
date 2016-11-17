package sgload

import (
	"bytes"
	"fmt"
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

	// Create writers
	writers, err := wlr.createWriters(&wg)
	if err != nil {
		return err
	}
	writerAgentIds := getWriterAgentIds(writers)

	channelNames := wlr.generateChannelNames()
	docsToChannelsAndWriters := createAndAssignDocs(
		writerAgentIds,
		channelNames,
		wlr.WriteLoadSpec.NumDocs,
		wlr.WriteLoadSpec.DocSizeBytes,
		wlr.WriteLoadSpec.TestSessionID,
	)

	// Update writer with expected docs list
	for _, writer := range writers {
		expectedDocs := docsToChannelsAndWriters[writer.UserCred.Username]
		writer.SetExpectedDocsWritten(expectedDocs)
	}

	// Create writer goroutines
	for _, writer := range writers {
		go writer.Run()
	}

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
			AgentSpec{
				FinishedWg:            wg,
				UserCred:              userCred,
				ID:                    userId,
				DataStore:             dataStore,
				BatchSize:             wlr.WriteLoadSpec.BatchSize,
				ExpvarProgressEnabled: wlr.LoadRunner.LoadSpec.ExpvarProgressEnabled,
			},
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

func (wlr WriteLoadRunner) feedDocsToWriters(writers []*Writer, docsToChannelsAndWriters map[string][]Document) error {

	// Loop over doc assignment map and tell each writer to push to data store
	for writerAgentUsername, docsToWrite := range docsToChannelsAndWriters {
		writer := findWriterByAgentUsername(writers, writerAgentUsername)
		logger.Info(
			"Feeding docs to writer",
			"numdocs",
			len(docsToWrite),
			"writer",
			writer.Agent.UserCred.Username,
		)
		writer.AddToDataStore(docsToWrite)
	}

	// Send terminal docs which will shutdown writers after they've
	// processed all the normal docs
	for _, writer := range writers {
		logger.Info("Feeding terminal doc to writer", "writer", writer.Agent.UserCred.Username)
		d := Document{}
		d["_terminal"] = true
		writer.AddToDataStore([]Document{d})
	}

	return nil

}

func findWriterByAgentUsername(writers []*Writer, writerAgentUsername string) *Writer {

	for _, writer := range writers {
		if writer.UserCred.Username == writerAgentUsername {
			return writer
		}
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

// Create body content as map of 100 byte entries.  Rounds up to the nearest 100 bytes
func createBodyContentAsMapWithSize(docSizeBytes int) map[string]string {

	numEntries := int(docSizeBytes/100) + 1
	body := make(map[string]string, numEntries)
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("field_%d", i)
		body[key] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}
	return body
}
