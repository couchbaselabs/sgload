package sgload

import (
	"expvar"
	"fmt"
	"sync"

	"github.com/peterbourgon/g2s"
)

type AgentSpec struct {
	FinishedWg *sync.WaitGroup // Allows interested party to block until agent is done
	UserCred
	ID                  int       // The numeric ID of the writer (ephemeral, only stored in memory)
	CreateDataStoreUser bool      // Whether this writer must first create a user on the DataStore service, ot just assume it already exists
	DataStore           DataStore // The target data store where docs will be written
	BatchSize           int       // bulk_get or bulk_docs batch size
}

// Contains common fields and functionality between readers and writers
type Agent struct {
	AgentSpec
	StatsdClient *g2s.Statsd // The statsd client instance to use to push stats to statdsd
	ExpVarStats  *expvar.Map // The expvar progress stats map for this agent
}

func (a *Agent) createSGUserIfNeeded(channels []string) {

	if a.CreateDataStoreUser == true {

		logger.Info("Creating SG user", "username", a.UserCred.Username, "channels", channels)

		if err := a.DataStore.CreateUser(a.UserCred, channels); err != nil {
			panic(fmt.Sprintf("Error creating user in datastore.  User: %v, Err: %v", a.UserCred, err))
		}
	}

}

func (a *Agent) SetStatsdClient(statsdClient *g2s.Statsd) {
	a.StatsdClient = statsdClient
}

func (a *Agent) setupExpVarStats(expvarMap *expvar.Map) {
	expVarStats := &expvar.Map{}
	expVarStats.Init()
	a.ExpVarStats = expVarStats
	expvarMap.Set(a.Username, expVarStats)
}
