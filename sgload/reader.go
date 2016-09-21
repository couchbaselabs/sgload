package sgload

import (
	"fmt"
	"strings"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
	"github.com/peterbourgon/g2s"
)

type Reader struct {
	Agent
	SGChannels      []string // The Sync Gateway channels this reader is assigned to pull from
	NumDocsExpected int      // The total number of docs this reader is expected to pull
	BatchSize       int      // The number of docs to pull in batch (_changes feed and bulk_get)

}

func NewReader(wg *sync.WaitGroup, ID int, u UserCred, d DataStore, batchsize int) *Reader {

	reader := Reader{
		Agent: Agent{
			FinishedWg: wg,
			UserCred:   u,
			ID:         ID,
			DataStore:  d,
			BatchSize:  batchsize,
		},
	}

	return &reader

}

func (r *Reader) SetChannels(sgChannels []string) {
	r.SGChannels = sgChannels
}

func (r *Reader) SetNumDocsExpected(n int) {
	r.NumDocsExpected = n
}

func (r *Reader) SetBatchSize(batchSize int) {
	r.BatchSize = batchSize
}

func (r *Reader) SetStatsdClient(statsdClient *g2s.Statsd) {
	r.StatsdClient = statsdClient
}

func (r *Reader) pushPostRunTimingStats(numDocsPulled int, timeStartedCreatingDocs time.Time) {
	if r.StatsdClient == nil {
		return
	}
	delta := time.Since(timeStartedCreatingDocs)

	// How long it took for this reader to read all of its docs
	r.StatsdClient.Timing(
		statsdSampleRate,
		"get_all_documents",
		delta,
	)

	if numDocsPulled > 0 {
		// Average time it took to read each doc from
		// the changes feed and the doc itself
		deltaChangeAndDoc := time.Duration(int64(delta) / int64(numDocsPulled))
		r.StatsdClient.Timing(
			statsdSampleRate,
			"get_change_and_document",
			deltaChangeAndDoc,
		)
	}

	logger.Info("Reader finished", "agent.ID", r.ID, "numdocs", numDocsPulled, "get_all_documents", delta)

}

func (r *Reader) Run() {

	numDocsPulled := 0
	since := StringSincer{}
	result := pullMoreDocsResult{}
	var err error
	var timeStartedCreatingDocs time.Time

	defer r.FinishedWg.Done()
	defer func() {
		r.pushPostRunTimingStats(numDocsPulled, timeStartedCreatingDocs)
	}()

	r.createSGUserIfNeeded()

	timeStartedCreatingDocs = time.Now()

	for {

		if r.isFinished(numDocsPulled) {
			break
		}
		result, err = r.pullMoreDocs(since)
		if err != nil {
			panic(fmt.Sprintf("Got error getting changes: %v", err))
		}
		since = result.since
		numDocsPulled += result.numDocsPulled

	}

}

func (r *Reader) createSGUserIfNeeded() {
	if r.CreateDataStoreUser == true {

		logger.Info("Creating reader SG user", "username", r.UserCred.Username, "channels", r.SGChannels)

		if err := r.DataStore.CreateUser(r.UserCred, r.SGChannels); err != nil {
			panic(fmt.Sprintf("Error creating user in datastore.  User: %v, Err: %v", r.UserCred, err))
		}
	}

}

func (r *Reader) isFinished(numDocsPulled int) bool {

	logger.Info("Reader checking if finished", "agent.ID", r.ID, "numDocsPulled", numDocsPulled, "numDocsExpected", r.NumDocsExpected)

	switch {
	case numDocsPulled > r.NumDocsExpected:
		panic(fmt.Sprintf("Reader was only expected to pull %d docs, but pulled %d.", r.NumDocsExpected, numDocsPulled))
	case numDocsPulled == r.NumDocsExpected:
		return true
	default:
		return false
	}

}

type pullMoreDocsResult struct {
	since         StringSincer
	numDocsPulled int
}

func (r *Reader) pullMoreDocs(since Sincer) (pullMoreDocsResult, error) {

	// Create a retry sleeper which controls how many times to retry
	// and how long to wait in between retries
	numRetries := 7
	sleepMsBetweenRetry := 500
	retrySleeper := CreateDoublingSleeperFunc(numRetries, sleepMsBetweenRetry)

	// Create retry worker that knows how to do actual work
	retryWorker := func() (shouldRetry bool, err error, value interface{}) {

		result := pullMoreDocsResult{}

		changes, newSince, err := r.DataStore.Changes(since, r.BatchSize)
		if err != nil {
			return false, err, result
		}

		if len(changes.Results) == 0 {
			logger.Warn("No changes pulled", "agent.ID", r.ID, "since", since)
			return true, nil, result
		}
		if newSince.Equals(since) {
			logger.Warn("Since value should have changed", "agent.ID", r.ID, "since", since, "newsince", newSince)
			return true, nil, result
		}

		// Strip out any changes with id "id":"_user/*"
		// since they are user docs and we don't care about them
		changes = stripUserDocChanges(changes)

		bulkGetRequest := getBulkGetRequest(changes)

		err = r.DataStore.BulkGetDocuments(bulkGetRequest)
		if err != nil {
			return false, err, result
		}

		result.since = newSince.(StringSincer)
		result.numDocsPulled = len(bulkGetRequest.Docs)
		return false, nil, result

	}

	// Invoke the retry worker / sleeper combo in a loop
	err, workerReturnVal := RetryLoop("pullMoreDocs", retryWorker, retrySleeper)
	if err != nil {
		return pullMoreDocsResult{}, err
	}

	return workerReturnVal.(pullMoreDocsResult), nil

}

func getBulkGetRequest(changes sgreplicate.Changes) sgreplicate.BulkGetRequest {

	bulkDocsRequest := sgreplicate.BulkGetRequest{}
	docs := []sgreplicate.DocumentRevisionPair{}
	for _, change := range changes.Results {
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = change.Id
		docRevPair.Revision = change.ChangedRevs[0].Revision
		docs = append(docs, docRevPair)
	}
	bulkDocsRequest.Docs = docs
	return bulkDocsRequest

}

func stripUserDocChanges(changes sgreplicate.Changes) (changesStripped sgreplicate.Changes) {
	changesStripped.LastSequence = changes.LastSequence

	for _, change := range changes.Results {
		if strings.Contains(change.Id, "_user") {
			continue
		}
		changesStripped.Results = append(changesStripped.Results, change)

	}

	return changesStripped
}

// A retry sleeper is called back by the retry loop and passed
// the current retryCount, and should return the amount of milliseconds
// that the retry should sleep.
type RetrySleeper func(retryCount int) (shouldContinue bool, timeTosleepMs int)

// A RetryWorker encapsulates the work being done in a Retry Loop.  The shouldRetry
// return value determines whether the worker will retry, regardless of the err value.
// If the worker has exceeded it's retry attempts, then it will not be called again
// even if it returns shouldRetry = true.
type RetryWorker func() (shouldRetry bool, err error, value interface{})

func RetryLoop(description string, worker RetryWorker, sleeper RetrySleeper) (error, interface{}) {

	numAttempts := 1

	for {
		shouldRetry, err, value := worker()
		if !shouldRetry {
			if err != nil {
				return err, nil
			}
			return nil, value
		}
		shouldContinue, sleepMs := sleeper(numAttempts)
		if !shouldContinue {
			if err == nil {
				err = fmt.Errorf("RetryLoop for %v giving up after %v attempts", description, numAttempts)
			}
			logger.Warn("RetryLoop giving up", "description", description, "numAttempts", numAttempts)
			return err, value
		}
		logger.Warn("RetryLoop will retry soon", "description", description, "sleepMs", sleepMs)

		<-time.After(time.Millisecond * time.Duration(sleepMs))

		numAttempts += 1

	}
}

// Create a RetrySleeper that will double the retry time on every iteration and
// use the given parameters
func CreateDoublingSleeperFunc(maxNumAttempts, initialTimeToSleepMs int) RetrySleeper {

	timeToSleepMs := initialTimeToSleepMs

	sleeper := func(numAttempts int) (bool, int) {
		if numAttempts > maxNumAttempts {
			return false, -1
		}
		if numAttempts > 1 {
			timeToSleepMs *= 2
		}
		return true, timeToSleepMs
	}
	return sleeper

}
