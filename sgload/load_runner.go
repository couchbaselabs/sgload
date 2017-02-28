package sgload

import (
	"fmt"

	"github.com/peterbourgon/g2s"
)

type LoadRunner struct {
	LoadSpec     LoadSpec
	StatsdClient g2s.Statter
}

func (lr *LoadRunner) CreateStatsdClient() {

	var statsdClient g2s.Statter
	var err error

	if lr.LoadSpec.StatsdEnabled {
		// statsClient *should* be safe to be shared among multiple
		// goroutines, based on fact that connection returned from Dial
		if lr.LoadSpec.StatsdPrefix != "" {
			statsdClient, err = g2s.DialWithPrefix("udp", lr.LoadSpec.StatsdEndpoint, lr.LoadSpec.StatsdPrefix)
		} else {
			statsdClient, err = g2s.Dial("udp", lr.LoadSpec.StatsdEndpoint)
		}

		if err != nil {
			panic("Couldn't connect to statsd!")
		}
	} else {
		statsdClient = MockStatter{}
	}

	lr.StatsdClient = statsdClient

}

func (lr LoadRunner) createDataStore() DataStore {

	if lr.LoadSpec.MockDataStore {
		return NewMockDataStore()
	}

	sgDataStore := NewSGDataStore(
		lr.LoadSpec.SyncGatewayUrl,
		lr.LoadSpec.SyncGatewayAdminPort,
		lr.StatsdClient,
		lr.LoadSpec.CompressionEnabled,
	)

	return sgDataStore

}

func (lr LoadRunner) generateChannelNames() []string {
	channelNames := []string{}
	for i := 0; i < lr.LoadSpec.NumChannels; i++ {
		channelName := fmt.Sprintf("%d-%s", i, lr.LoadSpec.TestSessionID)
		channelNames = append(
			channelNames,
			channelName,
		)
	}
	return channelNames
}

func (lr LoadRunner) generateUserCreds(numUsers int, usernamePrefix string) []UserCred {
	return lr.LoadSpec.generateUserCreds(numUsers, usernamePrefix)
}

func (lr LoadRunner) loadUserCredsFromArgs(numUsers int, usernamePrefix string) ([]UserCred, error) {

	userCreds := []UserCred{}
	var err error

	switch {
	case lr.LoadSpec.TestSessionID != "":
		// If the user explicitly provided a test session ID, then use that
		// to generate user credentials to use.  Presumably these credentials
		// were created before in previous runs.  Doesn't make sense to use
		// this with auto-generated test session ID's, since there is no way
		// that the Sync Gateway will have those users created from prev. runs
		userCreds = lr.generateUserCreds(numUsers, usernamePrefix)
	default:
		return userCreds, fmt.Errorf("You need to either create load generator users explicitly or specify a test session ID.  See CLI help.")

	}

	return userCreds, err
}
