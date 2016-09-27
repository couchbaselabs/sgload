package sgload

import (
	"fmt"
	"sync"
)

const (
	USER_PREFIX_UPDATER = "updater"
)

type UpdateLoadRunner struct {
	LoadRunner
	UpdateLoadSpec UpdateLoadSpec
}

func NewUpdateLoadRunner(uls UpdateLoadSpec) *UpdateLoadRunner {

	uls.MustValidate()

	logger.Info("creating loadrunner", "loadspec", fmt.Sprintf("%+v", uls.LoadSpec))
	loadRunner := LoadRunner{
		LoadSpec: uls.LoadSpec,
	}
	loadRunner.CreateStatsdClient()

	return &UpdateLoadRunner{
		LoadRunner:     loadRunner,
		UpdateLoadSpec: uls,
	}
}

func (ulr UpdateLoadRunner) Run() error {

	// Create a wait group to see when all the updater goroutines have finished
	var wg sync.WaitGroup

	// Create updater goroutines
	updaters, err := ulr.createUpdaters(&wg)
	if err != nil {
		return err
	}
	for _, updater := range updaters {
		go updater.Run()
	}

	channelNames := ulr.generateChannelNames()
	updaterAgentUsernames := getUpdaterAgentUsernames(updaters)
	docsToChannelsAndUpdaters := createAndAssignDocs(
		updaterAgentUsernames,
		channelNames,
		ulr.UpdateLoadSpec.NumDocs,
		ulr.UpdateLoadSpec.DocSizeBytes,
	)

	// Create doc feeder goroutine
	go ulr.feedDocsToUpdaters(updaters, docsToChannelsAndUpdaters)

	// Wait for updaters to finish
	logger.Info("Waiting for updaters to finish", "numupdaters", len(updaters))
	wg.Wait()
	logger.Info("Updaters finished")

	return nil

}

func getUpdaterAgentUsernames(updaters []*Updater) []string {
	updaterAgentIds := []string{}
	for _, updater := range updaters {
		updaterAgentIds = append(updaterAgentIds, updater.UserCred.Username)
	}
	return updaterAgentIds
}

func (ulr UpdateLoadRunner) createUpdaters(wg *sync.WaitGroup) ([]*Updater, error) {

	updaters := []*Updater{}
	var userCreds []UserCred
	var err error

	userCreds, err = ulr.loadUserCredsFromArgs(ulr.UpdateLoadSpec.NumUpdaters, USER_PREFIX_UPDATER)
	if err != nil {
		return updaters, err
	}

	for userId := 0; userId < ulr.UpdateLoadSpec.NumUpdaters; userId++ {
		userCred := userCreds[userId]
		dataStore := ulr.createDataStore()
		dataStore.SetUserCreds(userCred)
		updater := NewUpdater(
			wg,
			userId,
			userCred,
			dataStore,
		)
		updater.SetStatsdClient(ulr.StatsdClient)
		updaters = append(updaters, updater)
		wg.Add(1)
	}

	return updaters, nil

}

func (ulr UpdateLoadRunner) generateUserCreds() []UserCred {
	return ulr.LoadRunner.generateUserCreds(ulr.UpdateLoadSpec.NumUpdaters, USER_PREFIX_UPDATER)
}

func (ulr UpdateLoadRunner) feedDocsToUpdaters(updaters []*Updater, docsToChannelsAndUpdaters map[string][]Document) error {

	return nil

}
