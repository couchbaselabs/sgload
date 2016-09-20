package sgload

import (
	"fmt"

	"github.com/peterbourgon/g2s"
)

type LoadRunner struct {
	LoadSpec     LoadSpec
	StatsdClient *g2s.Statsd
}

func (lr *LoadRunner) CreateStatsdClient() {

	var statsdClient *g2s.Statsd
	var err error

	if lr.LoadSpec.StatsdEnabled {
		// statsClient *should* be safe to be shared among multiple
		// goroutines, based on fact that connection returned from Dial
		statsdClient, err = g2s.Dial("udp", lr.LoadSpec.StatsdEndpoint)
		if err != nil {
			panic("Couldn't connect to statsd!")
		}
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
