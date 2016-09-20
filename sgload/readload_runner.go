package sgload

import (
	"encoding/json"
	"fmt"
	"sync"
)

type ReadLoadRunner struct {
	LoadRunner
	ReadLoadSpec ReadLoadSpec
}

func NewReadLoadRunner(rls ReadLoadSpec) *ReadLoadRunner {

	rls.MustValidate()

	loadRunner := LoadRunner{
		LoadSpec: rls.LoadSpec,
	}
	loadRunner.CreateStatsdClient()

	return &ReadLoadRunner{
		LoadRunner:   loadRunner,
		ReadLoadSpec: rls,
	}

}

func (rlr ReadLoadRunner) Run() error {

	// Create a wait group to see when all the reader goroutines have finished
	var wg sync.WaitGroup

	// Create writer goroutines
	readers, err := rlr.createReaders(&wg)
	if err != nil {
		return fmt.Errorf("Error creating readers: %v", err)
	}
	for _, reader := range readers {
		go reader.Run()
	}

	// block until readers are done
	logger.Info("Waiting for readers to finish")
	wg.Wait()
	logger.Info("Readers finished")

	return nil

}

func (rlr ReadLoadRunner) createReaders(wg *sync.WaitGroup) ([]*Reader, error) {

	readers := []*Reader{}
	var userCreds []UserCred
	var err error

	switch rlr.ReadLoadSpec.CreateReaders {
	case true:
		userCreds = rlr.generateUserCreds()
	default:
		userCreds, err = rlr.loadUserCredsFromArgs()
		if err != nil {
			return readers, fmt.Errorf("Error loading user creds from args: %v", err)
		}
	}

	for userId := 0; userId < rlr.ReadLoadSpec.NumReaders; userId++ {
		userCred := userCreds[userId]
		dataStore := rlr.createDataStore()
		dataStore.SetUserCreds(userCred)

		// get channels that should be assigned to this reader
		sgChannels := rlr.assignChannelsToReader(rlr.generateChannelNames())

		reader := NewReader(
			wg,
			userId,
			userCred,
			dataStore,
			rlr.ReadLoadSpec.BatchSize,
		)
		reader.SetChannels(sgChannels)
		reader.SetBatchSize(rlr.ReadLoadSpec.BatchSize)
		reader.SetNumDocsExpected(rlr.numDocsExpectedPerReader())
		reader.CreateDataStoreUser = rlr.ReadLoadSpec.CreateReaders
		readers = append(readers, reader)
		wg.Add(1)
	}

	return readers, nil
}

// TODO: duplicated code with WriteLoadRunner.loadUserCredsFromArgs()
func (rlr ReadLoadRunner) loadUserCredsFromArgs() ([]UserCred, error) {

	userCreds := []UserCred{}
	var err error

	switch {
	case rlr.ReadLoadSpec.ReaderCreds != "":
		logger.Info("Load writer creds from CLI args")
		err = json.Unmarshal([]byte(rlr.ReadLoadSpec.ReaderCreds), &userCreds)
		if err != nil {
			return userCreds, err
		}
		for _, userCred := range userCreds {
			if userCred.Empty() {
				return userCreds, fmt.Errorf("User credentials empty: %+v", userCred)
			}
		}
	case rlr.ReadLoadSpec.TestSessionID != "" && rlr.ReadLoadSpec.DidAutoGenTestSessionID == false:
		// If the user explicitly provided a test session ID, then use that
		// to generate user credentials to use.  Presumably these credentials
		// were created before in previous runs.  Doesn't make sense to use
		// this with auto-generated test session ID's, since there is no way
		// that the Sync Gateway will have those users created from prev. runs
		logger.Info("Generate user creds from test session id")
		userCreds = rlr.generateUserCreds()
	default:
		return userCreds, fmt.Errorf("You need to either create writers, specify a test session ID, or specify writer user credentials.  See CLI help.")

	}

	if len(userCreds) != rlr.ReadLoadSpec.NumReaders {
		return userCreds, fmt.Errorf("You only provided %d user credentials, but specified %d readers", len(userCreds), rlr.ReadLoadSpec.NumReaders)
	}

	return userCreds, err
}

// Calculate how many docs each reader is expected to pull.  Find out how many docs are
// in each channel, and then find out how many channels each reader is pulling from,
// and then multiply to get the number docs each reader is expected to pull.
func (rlr ReadLoadRunner) numDocsExpectedPerReader() int {

	numDocsPerChannel := rlr.ReadLoadSpec.NumDocs / rlr.ReadLoadSpec.NumChannels
	docsPerReader := numDocsPerChannel * rlr.ReadLoadSpec.NumChansPerReader

	return docsPerReader

}

// Given the full list of SG channel names for this scenario, assign one more more
// SG channels to this particular reader.  This means that when the reader user is
// created, this will have these channels listed in their admin_channels field
// so they pull these channels when hittting the _changes feed.
func (rlr ReadLoadRunner) assignChannelsToReader(sgChannels []string) []string {

	assignedChannels := []string{}

	if rlr.ReadLoadSpec.NumChansPerReader > len(sgChannels) {
		panic(fmt.Sprintf("Cannot have more chans per reader (%d) than total channels (%d)", rlr.ReadLoadSpec.NumChansPerReader, len(sgChannels)))
	}

	for i := 0; i < rlr.ReadLoadSpec.NumChansPerReader; i++ {
		sgChannel := sgChannels[i]
		assignedChannels = append(assignedChannels, sgChannel)
	}

	return assignedChannels

}

func (rlr ReadLoadRunner) generateUserCreds() []UserCred {
	return rlr.LoadRunner.generateUserCreds(rlr.ReadLoadSpec.NumReaders, "readload")
}
