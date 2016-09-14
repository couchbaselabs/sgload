package sgload

import (
	"fmt"

	"github.com/peterbourgon/g2s"
)

type ReadLoadRunner struct {
	LoadRunner
	ReadLoadSpec ReadLoadSpec
	StatsdClient *g2s.Statsd
}

func NewReadLoadRunner(rls ReadLoadSpec) *ReadLoadRunner {

	var statsdClient *g2s.Statsd
	var err error

	if rls.StatsdEnabled {
		// statsClient *should* be safe to be shared among multiple
		// goroutines, based on fact that connection returned from Dial
		statsdClient, err = g2s.Dial("udp", rls.StatsdEndpoint)
		if err != nil {
			panic("Couldn't connect to statsd!")
		}
	}

	rls.MustValidate()

	return &ReadLoadRunner{
		ReadLoadSpec: rls,
		StatsdClient: statsdClient,
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
		reader := NewReader(
			userId,
			userCred,
			dataStore,
			rlr.ReadLoadSpec.BatchSize,
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
