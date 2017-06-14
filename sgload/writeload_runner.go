package sgload

import (
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

	AllSGUsersCreated := &sync.WaitGroup{}
	AllSGUsersCreated.Add(wlr.WriteLoadSpec.NumWriters)

	// Create writers
	writers, err := wlr.createWriters(&wg, AllSGUsersCreated)
	if err != nil {
		return err
	}

	channelNames := wlr.generateChannelNames()

	// Update writer with expected docs list
	approxDocsPerWriter := wlr.WriteLoadSpec.NumDocs / len(writers)
	for _, writer := range writers {
		writer.SetApproxExpectedDocsWritten(approxDocsPerWriter)
	}

	// Create writer goroutines
	for _, writer := range writers {
		go writer.Run()
	}

	// Create doc feeder goroutine
	go wlr.startDocFeeders(
		writers,
		wlr.WriteLoadSpec,
		approxDocsPerWriter,
		channelNames,
	)

	// Wait for writers to finish
	logger.Info("Waiting for writers to finish", "numwriters", len(writers))
	wg.Wait()
	logger.Info("Writers finished")

	return nil

}

func (wlr WriteLoadRunner) startDocFeeders(writers []*Writer, wls WriteLoadSpec, approxDocsPerWriter int, channelNames []string) error {
	// Create doc feeder goroutines
	for _, writer := range writers {
		go feedDocsToWriter(writer, wls, approxDocsPerWriter, channelNames)
	}
	return nil
}

func (wlr WriteLoadRunner) createWriters(wg, AllSGUsersCreated *sync.WaitGroup) ([]*Writer, error) {

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

		writerSpec := WriterSpec{
			DelayBetweenWrites: wlr.WriteLoadSpec.DelayBetweenWrites,
		}
		writer := NewWriter(
			AgentSpec{
				FinishedWg:              wg,
				UserCred:                userCred,
				ID:                      userId,
				DataStore:               dataStore,
				BatchSize:               wlr.WriteLoadSpec.BatchSize,
				ExpvarProgressEnabled:   wlr.LoadRunner.LoadSpec.ExpvarProgressEnabled,
				MaxConcurrentCreateUser: maxConcurrentCreateUser,
				AllSGUsersCreated:       AllSGUsersCreated,
				AttachSizeBytes:         wlr.LoadSpec.AttachSizeBytes,
			},
			writerSpec,
		)
		writer.SetStatsdClient(wlr.StatsdClient)
		writer.SetCreateUserSemaphore(createUserSemaphore)
		writer.CreateDataStoreUser = wlr.WriteLoadSpec.CreateWriters
		writers = append(writers, writer)
		wg.Add(1)
	}

	return writers, nil

}

func (wlr WriteLoadRunner) generateUserCreds() []UserCred {
	return wlr.LoadRunner.generateUserCreds(wlr.WriteLoadSpec.NumWriters, USER_PREFIX_WRITER)
}

func findWriterByAgentUsername(writers []*Writer, writerAgentUsername string) *Writer {

	for _, writer := range writers {
		if writer.UserCred.Username == writerAgentUsername {
			return writer
		}
	}
	return nil
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
