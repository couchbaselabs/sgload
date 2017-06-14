package sgload

import (
	"expvar"
	"fmt"
	"sync"

	"github.com/abiosoft/semaphore"
	"github.com/peterbourgon/g2s"
)

var (
	// Package-wide singleton semaphore to ensure max # of
	// outstanding concurrent createuser requests
	createUserSemaphore = semaphore.New(maxConcurrentCreateUser)
)

type AgentSpec struct {
	FinishedWg *sync.WaitGroup // Allows interested party to block until agent is done
	UserCred
	ID                      int             // The numeric ID of the writer (ephemeral, only stored in memory)
	CreateDataStoreUser     bool            // Whether this writer must first create a user on the DataStore service, ot just assume it already exists
	DataStore               DataStore       // The target data store where docs will be written
	BatchSize               int             // bulk_get or bulk_docs batch size
	AttachSizeBytes         int             // If > 0, and BatchSize == 1, then it will add attachments of this size during doc creates/updates.
	ExpvarProgressEnabled   bool            // Whether to publish reader/writer/updater progress to expvars
	MaxConcurrentCreateUser int             // The maximum number of concurrent outstanding createuser requests.  0 means no maximum
	AllSGUsersCreated       *sync.WaitGroup // Wait Group to allow waiting until all SG users created before applying load
}

// Contains common fields and functionality between readers and writers
type Agent struct {
	AgentSpec
	StatsdClient        g2s.Statter          // The statsd client instance to use to push stats to statdsd
	ExpVarStats         ExpVarStatsCollector // The expvar progress stats map for this agent
	CreateUserSemaphore *semaphore.Semaphore // Semaphore to ensure max # of concrrent createuser requests
	CreatedSGUser       bool                 // State to track whether SG user has already been created
}

func (a *Agent) createSGUserIfNeeded(channels []string) {

	if a.CreateDataStoreUser != true {
		return
	}

	if a.CreatedSGUser == true {
		return
	}

	if a.MaxConcurrentCreateUser > 0 {

		// grab semaphore (or block)
		a.CreateUserSemaphore.Acquire()

		// defer release semaphore
		defer a.CreateUserSemaphore.Release()

	}

	if err := a.DataStore.CreateUser(a.UserCred, channels); err != nil {
		panic(fmt.Sprintf("Error creating user in datastore.  User: %v, Err: %v", a.UserCred, err))
	}

	a.CreatedSGUser = true

	logger.Info("Created SG user", "username", a.UserCred.Username, "channels", channels)

	a.AllSGUsersCreated.Done()

}

func (a *Agent) waitUntilAllSGUsersCreated() {
	logger.Debug("Wait until all SG users created", "username", a.Username)
	defer logger.Info("Agent is starting after waiting for all SG users to be added", "username", a.Username)
	a.AllSGUsersCreated.Wait()
}

func (a *Agent) SetStatsdClient(statsdClient g2s.Statter) {
	a.StatsdClient = statsdClient
}

func (a *Agent) setupExpVarStats(expvarMap *expvar.Map) {
	switch a.ExpvarProgressEnabled {
	case true:
		expVarStats := &expvar.Map{}
		expVarStats.Init()
		a.ExpVarStats = expVarStats
		expvarMap.Set(a.Username, expVarStats)
	default:
		a.ExpVarStats = NoOpExpvarStatsCollector{}
	}

}

func (a *Agent) SetCreateUserSemaphore(sem *semaphore.Semaphore) {
	a.CreateUserSemaphore = sem
}
