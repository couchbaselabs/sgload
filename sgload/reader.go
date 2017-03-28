package sgload

import (
	"fmt"
	"strings"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type Reader struct {
	Agent
	SGChannels                []string // The Sync Gateway channels this reader is assigned to pull from
	NumDocsExpected           int      // The total number of docs this reader is expected to pull'
	NumRevGenerationsExpected int      // The expected generate that each doc is expected to reach
	BatchSize                 int      // The number of docs to pull in batch (_changes feed and bulk_get)
	lastNumRevs               int
	feedType                  ChangesFeedType // Whether to use "feedtype=normal" or "feedtype=longpoll"

}

const (
	CHANGES_LIMIT = 100
)

func NewReader(agentSpec AgentSpec) *Reader {

	reader := Reader{
		Agent: Agent{
			AgentSpec: agentSpec,
		},
		NumRevGenerationsExpected: 1,
		feedType:                  FEED_TYPE_LONGPOLL,
	}

	reader.setupExpVarStats(readersProgressStats)

	return &reader

}

func (r *Reader) SetFeedType(feedType ChangesFeedType) {
	r.feedType = feedType
}

func (r *Reader) SetChannels(sgChannels []string) {
	r.SGChannels = sgChannels
}

func (r *Reader) SetNumDocsExpected(n int) {
	r.NumDocsExpected = n

}

func (r *Reader) SetNumRevGenerationsExpected(n int) {
	r.NumRevGenerationsExpected = n
	totalRevsExpected := int64(r.NumDocsExpected * r.NumRevGenerationsExpected)
	r.ExpVarStats.Add(
		"TotalRevsExpected",
		totalRevsExpected,
	)
	globalProgressStats.Add("TotalNumRevsPulledExpected", totalRevsExpected)

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

// Main loop of reader goroutine
func (r *Reader) Run() {

	since := StringSincer{}
	result := pullMoreDocsResult{}
	latestDocIdRevs := map[string]int{}
	var err error
	var timeStartedCreatingDocs time.Time

	defer r.FinishedWg.Done()
	defer func() {
		r.pushPostRunTimingStats(len(latestDocIdRevs), timeStartedCreatingDocs)
	}()
	defer func() {
		logger.Info(
			"Reader finished",
			"reader",
			r.Agent.UserCred.Username,
		)
	}()

	r.createReaderSGUserIfNeeded()

	r.waitUntilAllSGUsersCreated()

	timeStartedCreatingDocs = time.Now()

	for {

		if r.isFinished(latestDocIdRevs) {
			break
		}
		result, err = r.pullMoreDocs(since)
		if err != nil {
			logger.Error("Error calling pullMoreDocs", "agent.ID", r.ID, "since", since, "err", err)
			panic(fmt.Sprintf("Error calling pullMoreDocs: %v", err))
		}

		if len(result.uniqueDocIds) > 0 {
			logger.Debug(
				"Reader pulled more docs",
				"reader",
				r.Agent.UserCred.Username,
				"pulled",
				len(result.uniqueDocIds),
				"totalpulled",
				len(latestDocIdRevs),
				"expecteddocs",
				r.NumDocsExpected,
				"numRevGenerationsExpected",
				r.NumRevGenerationsExpected,
				"req since",
				since,
				"new since",
				result.since,
			)
		}

		// Increment the since so that it's used on the next changes feed request
		since = result.since

		err = storeLatestDocRev(latestDocIdRevs, result)
		if err != nil {
			panic(fmt.Sprintf("Error geting the latest docs and revisions: %v", err))
		}

	}

}

func (r *Reader) createReaderSGUserIfNeeded() {
	defer globalProgressStats.Add("NumReaderUsers", 1)
	r.createSGUserIfNeeded(r.SGChannels)
}

func getNumRevs(latestDocIdRevs map[string]int) int {
	numRevs := 0
	for _, generation := range latestDocIdRevs {
		numRevs += generation
	}
	return numRevs
}

func (r *Reader) isFinished(latestDocIdRevs map[string]int) bool {

	if len(latestDocIdRevs) > r.NumDocsExpected {
		panic(fmt.Sprintf("Reader was only expected to pull %d docs, but pulled %d.", r.NumDocsExpected, len(latestDocIdRevs)))
	}

	numRevs := getNumRevs(latestDocIdRevs)
	delta := numRevs - r.lastNumRevs
	r.ExpVarStats.Add(
		"NumLatestDocIdRevs",
		int64(delta),
	)
	globalProgressStats.Add("TotalNumRevsPulled", int64(delta))
	r.lastNumRevs = numRevs

	// Haven't seen all expected docs yet
	if len(latestDocIdRevs) < r.NumDocsExpected {
		return false
	}

	// We have seen all docs, verify that the revs are expected generation
	for docId, generation := range latestDocIdRevs {

		if generation > r.NumRevGenerationsExpected {
			panic(
				fmt.Sprintf(
					"Pulled generation (%d) larger than expected generation (%d).",
					generation,
					r.NumRevGenerationsExpected,
				),
			)
		}

		if generation < r.NumRevGenerationsExpected {
			logger.Debug(
				"Reader still waiting for revs",
				"reader",
				r.Agent.ID,
				"docId",
				docId,
				"generation",
				generation,
				"expected-generation",
				r.NumRevGenerationsExpected,
			)
			return false
		}

	}

	// We have found the expected number of docs and each doc has the expected rev generation
	return true

}

type pullMoreDocsResult struct {
	since        StringSincer
	uniqueDocIds map[string]sgreplicate.DocumentRevisionPair
}

func (r *Reader) pullMoreDocs(since Sincer) (pullMoreDocsResult, error) {

	// Create a retry sleeper which controls how many times to retry
	// and how long to wait in between retries
	maxRetries := 10
	numRetries := 0
	sleepMsBetweenRetry := 500
	retrySleeper := CreateDoublingSleeperFunc(maxRetries, sleepMsBetweenRetry)

	// Create retry worker that knows how to do actual work
	retryWorker := func() (shouldRetry bool, err error, value interface{}) {

		numRetries += 1

		result := pullMoreDocsResult{}

		changes, newSince, changesErr := r.DataStore.Changes(since, CHANGES_LIMIT, r.feedType)
		if changesErr != nil {
			logger.Warn("Error getting changes.  Retrying.",
				"since",
				since,
				"feedtype",
				r.feedType,
				"limit",
				CHANGES_LIMIT,
				"agent.ID",
				r.ID,
				"numRetries",
				numRetries,
				"error",
				changesErr,
			)
			return true, changesErr, result
		}

		if len(changes.Results) == 0 {
			logger.Warn("Got empty changes.  Retrying.", "agent.ID", r.ID)
			return true, nil, result
		}
		if newSince.Equals(since) {
			logger.Warn("Since value has not changed since last _changes request.  Ignorning and retrying.", "agent.ID", r.ID, "since", since, "newsince", newSince, "changes", changes)
			return true, nil, result
		}

		// Strip out any changes with id "id":"_user/*"
		// since they are user docs and we don't care about them
		changes = stripUserDocChanges(changes)

		bulkGetRequest, uniqueDocIds, bulkGetErr := createBulkGetRequest(changes)
		if bulkGetErr != nil {
			logger.Warn("Error creating bulk get request from _changes result.  Retrying", "reader", r.Agent.ID, "err", err)
			return true, nil, result
		}

		docs, bulkDocsErr := r.DataStore.BulkGetDocuments(bulkGetRequest)
		if bulkDocsErr != nil {
			return false, bulkDocsErr, result
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

func createBulkGetRequest(changes sgreplicate.Changes) (sgreplicate.BulkGetRequest, map[string]sgreplicate.DocumentRevisionPair, error) {

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

		err := fmt.Errorf("Unexpected unumber of uniqueDocsIds in changes feed")

		return bulkGetRequest, uniqueDocIds, err

	}

	return bulkGetRequest, uniqueDocIds, nil

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

// TODO: storing every single docid is relatively expensive:
//
//     flat  flat%   sum%        cum   cum%
// 1476.59MB 44.86% 44.86%  1477.09MB 44.87%  encoding/json.(*decodeState).literalStore
// 1047.14MB 31.81% 76.67%  1048.14MB 31.84%  github.com/couchbaselabs/sgload/sgload.storeLatestDocRev
func storeLatestDocRev(latestDocIdRevs map[string]int, r pullMoreDocsResult) error {

	for docId, docRevPair := range r.uniqueDocIds {
		generation, err := docRevPair.GetGeneration()
		if err != nil {
			return err
		}

		existingGeneration, ok := latestDocIdRevs[docId]
		if ok {
			// This should never happen
			if generation < existingGeneration {
				panic(
					fmt.Sprintf("New new generation (%d) was less than existing generation (%d)",
						generation,
						existingGeneration,
					),
				)
			}
		}

		latestDocIdRevs[docId] = generation
	}
	return nil

}
