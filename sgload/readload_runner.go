package sgload

import (
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
		userCreds, err = rlr.ReadLoadSpec.loadUserCredsFromArgs()
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
