package sgload

import (
	"fmt"
	"sync"
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

	// Since the UpdateLoad Runner assumes that all docs have already been
	// inserted, it simply feeds all docs to the updaters.  In the gateload
	// scenario, the writers will be pushing these docs as they are written.
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

	userCreds, err = ulr.loadUserCredsFromArgs(ulr.UpdateLoadSpec.NumUpdaters, USER_PREFIX_WRITER)
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
	return ulr.LoadRunner.generateUserCreds(ulr.UpdateLoadSpec.NumUpdaters, USER_PREFIX_WRITER)
}

func (ulr UpdateLoadRunner) feedDocsToUpdaters(updaters []*Updater, docsToChannelsAndUpdaters map[string][]Document) error {

	// Loop over doc assignment map and tell each updater to push to data store
	for updaterAgentUsername, docsToWrite := range docsToChannelsAndUpdaters {
		updater := findUpdaterByAgentUsername(updaters, updaterAgentUsername)
		docRevPairs, err := updater.LookupCurrentRevisions(docsToWrite)
		if err != nil {
			return err
		}
		updater.NotifyDocsInserted(docRevPairs)
	}

	return nil

}

func findUpdaterByAgentUsername(updaters []*Updater, updaterAgentUsername string) *Updater {

	for _, updater := range updaters {
		if updater.UserCred.Username == updaterAgentUsername {
			return updater
		}
	}
	return nil
}
