package sgload

import (
	"fmt"
	"sync"

	"github.com/couchbaselabs/sg-replicate"
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

	// Create updater goroutines
	var pushedDocsChan chan []sgreplicate.DocumentRevisionPair
	updaters, err := ulr.createUpdaters(&wg, userCreds, ulr.UpdateLoadSpec.NumDocs, pushedDocsChan)
	if err != nil {
		return err
	}
	for _, updater := range updaters {
		go updater.Run()
	}

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

func (ulr UpdateLoadRunner) createUpdaters(wg *sync.WaitGroup, userCreds []UserCred, numUniqueDocsToUpdate int, docsToUpdate <-chan []sgreplicate.DocumentRevisionPair) ([]*Updater, error) {

	updaters := []*Updater{}

	for userId := 0; userId < ulr.UpdateLoadSpec.NumUpdaters; userId++ {
		userCred := userCreds[userId]
		dataStore := ulr.createDataStore()
		dataStore.SetUserCreds(userCred)
		numUniqueDocsPerUpdater := numUniqueDocsToUpdate / ulr.UpdateLoadSpec.NumUpdaters

		updater := NewUpdater(
			AgentSpec{
				FinishedWg:            wg,
				UserCred:              userCred,
				ID:                    userId,
				DataStore:             dataStore,
				ExpvarProgressEnabled: ulr.LoadRunner.LoadSpec.ExpvarProgressEnabled,
			},
			numUniqueDocsPerUpdater,
			ulr.UpdateLoadSpec.NumUpdatesPerDoc,
			ulr.UpdateLoadSpec.BatchSize,
			ulr.UpdateLoadSpec.DocSizeBytes,
			ulr.UpdateLoadSpec.NumRevsPerUpdate,
			docsToUpdate,
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

func findUpdaterByAgentUsername(updaters []*Updater, updaterAgentUsername string) *Updater {

	for _, updater := range updaters {
		if updater.UserCred.Username == updaterAgentUsername {
			return updater
		}
	}
	return nil
}
