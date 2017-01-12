package sgload

import (
	"fmt"
	"math/rand"
	"sync"
)

const (
	USER_PREFIX_READER = "reader"
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

	// Create a wait group to allow agents to wait until all SG users are created.
	AllSGUsersCreated := &sync.WaitGroup{}
	AllSGUsersCreated.Add(rlr.ReadLoadSpec.NumReaders)

	// Create reader goroutines
	readers, err := rlr.createReaders(&wg, AllSGUsersCreated)
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

func (rlr ReadLoadRunner) createReaders(wg, AllSGUsersCreated *sync.WaitGroup) ([]*Reader, error) {

	readers := []*Reader{}
	var userCreds []UserCred
	var err error
	numDocsExpectedPerReader := rlr.numDocsExpectedPerReader()

	switch rlr.ReadLoadSpec.CreateReaders {
	case true:
		userCreds = rlr.generateUserCreds()
	default:
		userCreds, err = rlr.loadUserCredsFromArgs(rlr.ReadLoadSpec.NumReaders, USER_PREFIX_READER)
		if err != nil {
			return readers, fmt.Errorf("Error loading user creds from args: %v", err)
		}
	}

	for userId := 0; userId < rlr.ReadLoadSpec.NumReaders; userId++ {
		userCred := userCreds[userId]
		dataStore := rlr.createDataStore()
		dataStore.SetUserCreds(userCred)

		// get channels that should be assigned to this reader
		sgChannels := assignChannelsToReader(
			rlr.ReadLoadSpec.NumChansPerReader,
			rlr.generateChannelNames(), // TODO: pass this in rather than re-generating
		)

		agentSpec := AgentSpec{
			FinishedWg:              wg,
			UserCred:                userCred,
			ID:                      userId,
			DataStore:               dataStore,
			BatchSize:               rlr.ReadLoadSpec.BatchSize,
			ExpvarProgressEnabled:   rlr.LoadRunner.LoadSpec.ExpvarProgressEnabled,
			MaxConcurrentCreateUser: maxConcurrentCreateUser,
			AllSGUsersCreated:       AllSGUsersCreated,
		}

		reader := NewReader(agentSpec)
		reader.SetCreateUserSemaphore(createUserSemaphore)
		reader.SetFeedType(rlr.ReadLoadSpec.FeedType)
		reader.SetChannels(sgChannels)
		reader.SetBatchSize(rlr.ReadLoadSpec.BatchSize)
		reader.SetNumDocsExpected(numDocsExpectedPerReader)
		reader.SetNumRevGenerationsExpected(rlr.ReadLoadSpec.NumRevGenerationsExpected)
		reader.SetStatsdClient(rlr.StatsdClient)
		reader.CreateDataStoreUser = rlr.ReadLoadSpec.CreateReaders
		readers = append(readers, reader)
		wg.Add(1)
	}

	return readers, nil
}

// Calculate how many docs each reader is expected to pull.  Find out how many docs are
// in each channel, and then find out how many channels each reader is pulling from,
// and then multiply to get the number docs each reader is expected to pull.
func (rlr ReadLoadRunner) numDocsExpectedPerReader() int {

	// If we have 1000 docs total, and 10 channels, then there will be
	// 100 docs per channel (1000 / 10)
	numDocsPerChannel := rlr.ReadLoadSpec.NumDocs / rlr.ReadLoadSpec.NumChannels

	// If readers are subscribed to multiple channels, then they will expect
	// to read more documents from the changes feed
	docsPerReader := numDocsPerChannel * rlr.ReadLoadSpec.NumChansPerReader

	logger.Debug("DocsPerReader", "DocsPerReader", docsPerReader)

	return docsPerReader

}

// Given the full list of SG channel names for this scenario, assign one more more
// SG channels to this particular reader.  This means that when the reader user is
// created, this will have these channels listed in their admin_channels field
// so they pull these channels when hittting the _changes feed.
func assignChannelsToReader(numChansPerReader int, sgChannels []string) []string {

	assignedChannels := []string{}

	if numChansPerReader > len(sgChannels) {
		panic(fmt.Sprintf("Cannot have more chans per reader (%d) than total channels (%d)", numChansPerReader, len(sgChannels)))
	}

	for i := 0; i < numChansPerReader; i++ {
		for { // keep looping until we get a unique channel
			chanIndex := rand.Intn(len(sgChannels))
			sgChannel := sgChannels[chanIndex]
			if contains(assignedChannels, sgChannel) {
				continue
			}
			assignedChannels = append(assignedChannels, sgChannel)
			break
		}
	}

	return assignedChannels

}

func contains(stringslice []string, other string) bool {
	for _, s := range stringslice {
		if s == other {
			return true
		}
	}
	return false
}

func (rlr ReadLoadRunner) generateUserCreds() []UserCred {
	return rlr.LoadRunner.generateUserCreds(rlr.ReadLoadSpec.NumReaders, USER_PREFIX_READER)
}
