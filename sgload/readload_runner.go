package sgload

import (
	"fmt"
	"log"
	"time"

	"github.com/peterbourgon/g2s"
)

type ReadLoadRunner struct {
	LoadRunner
	ReadLoadSpec ReadLoadSpec
	StatsdClient *g2s.Statsd
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

	// Create writer goroutines
	readers, err := rlr.createReaders()
	if err != nil {
		return err
	}
	for _, reader := range readers {
		go reader.Run()
	}

	log.Printf("Created readers")
	time.Sleep(time.Second * 5)

	return nil

}

func (rlr ReadLoadRunner) createReaders() ([]*Reader, error) {
	readers := []*Reader{}
	var userCreds []UserCred
	var err error

	switch rlr.ReadLoadSpec.CreateReaders {
	case true:
		userCreds = rlr.generateUserCreds()
	default:
		userCreds, err = rlr.ReadLoadSpec.loadUserCredsFromArgs()
		if err != nil {
			return readers, err
		}
	}

	for userId := 0; userId < rlr.ReadLoadSpec.NumReaders; userId++ {
		userCred := userCreds[userId]
		dataStore := rlr.createDataStore()
		dataStore.SetUserCreds(userCred)

		// TODO: figure out reader to channels assignement
		sgChannels := []string{"foo"}

		reader := NewReader(
			userId,
			userCred,
			dataStore,
			rlr.ReadLoadSpec.BatchSize,
			sgChannels,
		)
		reader.CreateDataStoreUser = rlr.ReadLoadSpec.CreateReaders
		readers = append(readers, reader)
	}

	return readers, nil
}

func (rlr ReadLoadRunner) generateUserCreds() []UserCred {
	userCreds := []UserCred{}
	for userId := 0; userId < rlr.ReadLoadSpec.NumReaders; userId++ {
		username := fmt.Sprintf(
			"readload-user-%d-%s",
			userId,
			rlr.ReadLoadSpec.TestSessionID,
		)
		password := fmt.Sprintf(
			"readload-passw0rd-%d-%s",
			userId,
			rlr.ReadLoadSpec.TestSessionID,
		)
		userCred := UserCred{
			Username: username,
			Password: password,
		}
		userCreds = append(userCreds, userCred)

	}
	return userCreds

}
