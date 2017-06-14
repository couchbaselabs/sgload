package sgload

import (
	"sync"
)

type UpdateLoadRunner struct {
	LoadRunner
	UpdateLoadSpec UpdateLoadSpec
}

func (ulr UpdateLoadRunner) createUpdaters(wg *sync.WaitGroup, userCreds []UserCred, numUniqueDocsToUpdate int, docsToUpdate <-chan []DocumentMetadata) ([]*Updater, error) {

	updaters := []*Updater{}

	for userId := 0; userId < ulr.UpdateLoadSpec.NumUpdaters; userId++ {
		userCred := userCreds[userId]
		dataStore := ulr.createDataStore()
		dataStore.SetUserCreds(userCred)
		numUniqueDocsPerUpdater := numUniqueDocsToUpdate / ulr.UpdateLoadSpec.NumUpdaters

		updater := NewUpdater(
			AgentSpec{
				FinishedWg:              wg,
				UserCred:                userCred,
				ID:                      userId,
				DataStore:               dataStore,
				ExpvarProgressEnabled:   ulr.LoadRunner.LoadSpec.ExpvarProgressEnabled,
				MaxConcurrentCreateUser: maxConcurrentCreateUser,
				AttachSizeBytes:         ulr.LoadSpec.AttachSizeBytes,
			},
			numUniqueDocsPerUpdater,
			ulr.UpdateLoadSpec.NumUpdatesPerDoc,
			ulr.UpdateLoadSpec.BatchSize,
			ulr.UpdateLoadSpec.DocSizeBytes,
			ulr.UpdateLoadSpec.NumRevsPerUpdate,
			docsToUpdate,
			ulr.UpdateLoadSpec.DelayBetweenUpdates,
		)
		updater.SetStatsdClient(ulr.StatsdClient)
		updater.SetCreateUserSemaphore(createUserSemaphore)
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
