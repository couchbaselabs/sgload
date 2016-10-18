package sgload

import (
	"fmt"
	"strings"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
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

	logger.Info("Reader finished", "agent.ID", r.ID, "numdocs", numDocsPulled)

}

func (r *Reader) Run() {

	since := StringSincer{}
	result := pullMoreDocsResult{}
	uniqueDocIdsPulled := map[string]struct{}{}
	var err error
	var timeStartedCreatingDocs time.Time

	defer r.FinishedWg.Done()
	defer func() {
		r.pushPostRunTimingStats(len(uniqueDocIdsPulled), timeStartedCreatingDocs)
	}()

	r.createSGUserIfNeeded(r.SGChannels)

	timeStartedCreatingDocs = time.Now()

	for {

		if r.isFinished(len(uniqueDocIdsPulled)) {
			break
		}
		result, err = r.pullMoreDocs(since)
		if err != nil {
			logger.Error("Error calling pullMoreDocs", "agent.ID", r.ID, "since", since, "err", err)
			panic(fmt.Sprintf("Error calling pullMoreDoc: %v", err))
		}
		since = result.since

		addNewUniqueDocIdsPulled(uniqueDocIdsPulled, result)

	}

}

func (r *Reader) isFinished(numDocsPulled int) bool {

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
	since        StringSincer
	uniqueDocIds map[string]sgreplicate.DocumentRevisionPair
}

func (r *Reader) pullMoreDocs(since Sincer) (pullMoreDocsResult, error) {

	// Create a retry sleeper which controls how many times to retry
	// and how long to wait in between retries
	numRetries := 14
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
			return true, nil, result
		}
		if newSince.Equals(since) {
			logger.Warn("Since value should have changed", "agent.ID", r.ID, "since", since, "newsince", newSince)
			return true, nil, result
		}

		// Strip out any changes with id "id":"_user/*"
		// since they are user docs and we don't care about them
		changes = stripUserDocChanges(changes)

		bulkGetRequest, uniqueDocIds := getBulkGetRequest(changes)

		docs, err := r.DataStore.BulkGetDocuments(bulkGetRequest)
		if err != nil {
			return false, err, result
		}
		if len(docs) != len(bulkGetRequest.Docs) {
			return false, fmt.Errorf("Expected %d docs, got %d", len(bulkGetRequest.Docs), len(docs)), result
		}

		docsMustBeInExpectedChannels(docs, r.SGChannels)

		result.since = newSince.(StringSincer)
		result.uniqueDocIds = uniqueDocIds
		return false, nil, result

	}

	// Invoke the retry worker / sleeper combo in a loop
	err, workerReturnVal := RetryLoop("pullMoreDocs", retryWorker, retrySleeper)
	if err != nil {
		return pullMoreDocsResult{}, err
	}

	return workerReturnVal.(pullMoreDocsResult), nil

}

func getBulkGetRequest(changes sgreplicate.Changes) (sgreplicate.BulkGetRequest, map[string]sgreplicate.DocumentRevisionPair) {

	uniqueDocIds := map[string]sgreplicate.DocumentRevisionPair{}

	bulkGetRequest := sgreplicate.BulkGetRequest{}
	docs := []sgreplicate.DocumentRevisionPair{}
	for _, change := range changes.Results {
		docRevPair := sgreplicate.DocumentRevisionPair{}
		docRevPair.Id = change.Id
		docRevPair.Revision = change.ChangedRevs[0].Revision
		docs = append(docs, docRevPair)
		uniqueDocIds[docRevPair.Id] = docRevPair
	}
	bulkGetRequest.Docs = docs

	// Validate expectation that doc id's only appear once in the changes feed response
	if len(uniqueDocIds) != len(docs) {
		logger.Error(
			"len(uniqueDocIds) != len(docs)",
			"len(uniqueDocIds)",
			len(uniqueDocIds),
			"len(docs)",
			len(docs),
		)
		for docId, docRevPair := range uniqueDocIds {
			logger.Error("uniqueDocIds", "docId", docId, "docRevPair", docRevPair)
		}
		for _, doc := range docs {
			logger.Error("docs", "doc", doc, "doc.id", doc.Id, "doc.rev", doc.Revision)
		}
		panic(
			fmt.Sprintf(
				"len(uniqueDocIds) != len(docs), %d != %d",
				len(uniqueDocIds),
				len(docs),
			),
		)

	}

	return bulkGetRequest, uniqueDocIds

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
			return err, value
		}

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

func addNewUniqueDocIdsPulled(uniqueDocIdsPulled map[string]struct{}, r pullMoreDocsResult) {

	for docId, _ := range r.uniqueDocIds {
		uniqueDocIdsPulled[docId] = struct{}{}
	}

}
