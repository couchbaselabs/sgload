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

	// Create usercreds
	userCreds, err := ulr.createUserCreds()
	if err != nil {
		return err
	}

	// Generate the mapping between docs+channels and updaters
	channelNames := ulr.generateChannelNames()
	updaterAgentUsernames := getUpdaterAgentUsernames(userCreds)
	docsToChannelsAndUpdaters := createAndAssignDocs(
		updaterAgentUsernames,
		channelNames,
		ulr.UpdateLoadSpec.NumDocs,
		ulr.UpdateLoadSpec.DocSizeBytes,
		ulr.UpdateLoadSpec.TestSessionID,
	)

	// Create updater goroutines
	updaters, err := ulr.createUpdaters(&wg, userCreds, docsToChannelsAndUpdaters)
	if err != nil {
		return err
	}
	for _, updater := range updaters {
		go updater.Run()
	}

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

func getUpdaterAgentUsernames(userCreds []UserCred) []string {
	updaterAgentIds := []string{}
	for _, userCred := range userCreds {
		updaterAgentIds = append(updaterAgentIds, userCred.Username)
	}
	return updaterAgentIds
}

func (ulr UpdateLoadRunner) createUserCreds() ([]UserCred, error) {
	var userCreds []UserCred
	var err error

	userCreds, err = ulr.loadUserCredsFromArgs(
		ulr.UpdateLoadSpec.NumUpdaters,
		USER_PREFIX_WRITER, // re-use writer creds
	)
	if err != nil {
		return userCreds, err
	}
	return userCreds, nil

}

func (ulr UpdateLoadRunner) createUpdaters(wg *sync.WaitGroup, userCreds []UserCred, docMapping map[string][]Document) ([]*Updater, error) {

	updaters := []*Updater{}

	for userId := 0; userId < ulr.UpdateLoadSpec.NumUpdaters; userId++ {
		userCred := userCreds[userId]
		dataStore := ulr.createDataStore()
		dataStore.SetUserCreds(userCred)
		docsForUpdater := docMapping[userCred.Username]
		updater := NewUpdater(
			wg,
			userId,
			userCred,
			dataStore,
			ulr.UpdateLoadSpec.NumRevsPerDoc,
			docsForUpdater,
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
		updater.NotifyDocsReadyToUpdate(docRevPairs)
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
