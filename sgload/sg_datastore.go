package sgload

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/peterbourgon/g2s"
)

var (
	// The sample rate -- set this to 0.1 if you only want
	// 10% of the samples to be pushed to statds, or .01 if you only
	// want 1% of the samples pushed to statsd.  Useful for
	// not overwhelming stats if you have too many samples.
	statsdSampleRate float32 = 1.0

	initializeSgHttpClientOnce *sync.Once = &sync.Once{}

	sgClient *retryablehttp.Client
)

func initSgHttpClientOnce(statsdClient g2s.Statter) {

	// This is done _once_ because we only want one single client instance
	// that is shared among all of the goroutines

	doOnceFunc := func() {

		sgClient = retryablehttp.NewClient()

		// This is the loghook that is called back for retries.
		// Note that the exception to this is _bulk_docs responses
		// that have a 201 status code, but have embedded partial
		// failures.  In that case, a different mechanism must be
		// used since the retryablehttp only retries 5xx statuses
		logHook := func(
			ignoredLogger *log.Logger,
			req *http.Request,
			numAttempts int) {

			if numAttempts > 0 {
				logger.Warn(
					"HttpClientRetry",
					"url",
					req.URL,
					"numAttempts",
					numAttempts,
				)
				statsdClient.Counter(
					statsdSampleRate,
					"retries",
					1,
				)
			}

		}

		// Record retries in statsd
		sgClient.RequestLogHook = logHook

		// Suppress retryablehttp client logs because they are noisy and
		// don't use our structured logger.  To log retries, set a custom
		// CheckRetry function based on the retryablehttp DefaultRetryPolicy
		sgClient.Logger = log.New(ioutil.Discard, "", 0)

		sgClient.RetryMax = 10
		sgClient.HTTPClient.Transport = transportWithConnPool(1000)

	}

	initializeSgHttpClientOnce.Do(doOnceFunc)
}

type SGDataStore struct {
	SyncGatewayUrl       string
	SyncGatewayAdminPort int
	UserCreds            UserCred
	StatsdClient         g2s.Statter
	CompressionEnabled   bool
}

func NewSGDataStore(sgUrl string, sgAdminPort int, statsdClient g2s.Statter, compressionEnabled bool) *SGDataStore {

	initSgHttpClientOnce(statsdClient)

	return &SGDataStore{
		SyncGatewayUrl:       sgUrl,
		SyncGatewayAdminPort: sgAdminPort,
		StatsdClient:         statsdClient,
		CompressionEnabled:   compressionEnabled,
	}
}

func (s *SGDataStore) SetUserCreds(u UserCred) {
	s.UserCreds = u
}

func (s SGDataStore) CreateUser(u UserCred, channelNames []string) error {

	adminUrl, err := s.sgAdminURL()
	if err != nil {
		return err
	}

	adminUrlUserEndpoint, err := addEndpointToUrl(adminUrl, "_user")
	if err != nil {
		return err
	}

	adminUrlUserEndpoint = addTrailingSlash(adminUrlUserEndpoint)

	userDoc := map[string]interface{}{}
	userDoc["name"] = u.Username
	userDoc["password"] = u.Password
	userDoc["admin_channels"] = channelNames

	docBytes, err := json.Marshal(userDoc)
	if err != nil {
		return err
	}
	buf := bytes.NewReader(docBytes)

	req, err := retryablehttp.NewRequest("POST", adminUrlUserEndpoint, buf)

	req.Header.Set("Content-Type", "application/json")

	client := getHttpClient()

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	s.pushTimingStat("create_user", time.Since(startTime))

	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	io.Copy(ioutil.Discard, resp.Body)

	return nil
}

func (s SGDataStore) sgAdminURL() (string, error) {

	sgAdminURL := s.SyncGatewayUrl

	parsedSgUrl, err := url.Parse(s.SyncGatewayUrl)
	if err != nil {
		log.Printf("Error parsing url: %v", err)
		return sgAdminURL, err
	}

	// find the port from the url
	host, port, err := splitHostPortWrapper(parsedSgUrl.Host)
	if err != nil {
		return sgAdminURL, err
	}

	if port != "" {
		// is there a port?
		// do a regex replace on :port on the original url
		r := regexp.MustCompile(port)
		sgAdminURL = r.ReplaceAllString(
			sgAdminURL,
			fmt.Sprintf("%d", s.SyncGatewayAdminPort),
		)

	} else {
		// is there no port?
		// do a regex replace of host with host:port
		r := regexp.MustCompile(host)
		hostWithAdminPort := fmt.Sprintf("%v:%v", host, s.SyncGatewayAdminPort)
		sgAdminURL = r.ReplaceAllString(
			sgAdminURL,
			hostWithAdminPort,
		)

	}

	// return it
	return sgAdminURL, nil

}

func (s SGDataStore) Changes(sinceVal Sincer, limit int) (changes sgreplicate.Changes, newSinceVal Sincer, err error) {

	changesFeedEndpoint, err := addEndpointToUrl(s.SyncGatewayUrl, "_changes")
	if err != nil {
		return sgreplicate.Changes{}, sinceVal, err
	}

	changesFeedParams := NewChangesFeedParams(sinceVal, limit)

	changesFeedUrl := fmt.Sprintf(
		"%s?%s",
		changesFeedEndpoint,
		changesFeedParams,
	)

	req, err := retryablehttp.NewRequest("GET", changesFeedUrl, nil)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")

	client := getHttpClient()

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return sgreplicate.Changes{}, sinceVal, err
	}
	defer resp.Body.Close()

	s.pushTimingStat("changes_feed", time.Since(startTime))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return sgreplicate.Changes{}, sinceVal, fmt.Errorf("Unexpected response status for changes_feed GET request: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&changes)
	if err != nil {
		return sgreplicate.Changes{}, sinceVal, err
	}
	lastSequenceStr, ok := changes.LastSequence.(string)
	if !ok {
		return sgreplicate.Changes{}, sinceVal, fmt.Errorf("Could not convert changes.LastSequence to string")
	}
	lastSequenceSincer := StringSincer{
		Since: lastSequenceStr,
	}

	return changes, lastSequenceSincer, nil
}

// Create a single document in Sync Gateway
func (s SGDataStore) CreateDocument(d Document) ([]sgreplicate.DocumentRevisionPair, error) {

	return s.BulkCreateDocuments([]Document{d}, true)
}

// Bulk create a set of documents in Sync Gateway
func (s SGDataStore) BulkCreateDocuments(docs []Document, newEdits bool) ([]sgreplicate.DocumentRevisionPair, error) {

	defer s.pushCounter("create_document_counter", len(docs))

	// Set the "created_at" timestamp which is used to calculate the
	// gateload roundtrip time
	updateCreatedAtTimestamp(docs)

	bulkDocsResponse := []sgreplicate.DocumentRevisionPair{}

	bulkDocsEndpoint, err := addEndpointToUrl(s.SyncGatewayUrl, "_bulk_docs")
	if err != nil {
		return bulkDocsResponse, err
	}

	s.addDocBodies(docs)

	bulkDocs := BulkDocs{
		Documents: docs,
		NewEdits:  newEdits,
	}

	docBytes, err := json.Marshal(bulkDocs)
	if err != nil {
		return bulkDocsResponse, err
	}
	var reader *bytes.Reader
	if s.CompressionEnabled {
		buf := &bytes.Buffer{}
		gzipWriter := gzip.NewWriter(buf)
		if _, err := gzipWriter.Write(docBytes); err != nil {
			return bulkDocsResponse, err
		}
		if err = gzipWriter.Close(); err != nil {
			return bulkDocsResponse, err
		}
		compressedBytes, err := ioutil.ReadAll(buf)
		if err != nil {
			return bulkDocsResponse, err
		}
		reader = bytes.NewReader(compressedBytes)
	} else {
		reader = bytes.NewReader(docBytes)
	}

	req, err := retryablehttp.NewRequest("POST", bulkDocsEndpoint, reader)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")
	if s.CompressionEnabled {
		req.Header.Set("Content-Encoding", "gzip")
	}

	client := getHttpClient()

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return bulkDocsResponse, err
	}
	defer resp.Body.Close()

	s.pushTimingStat("create_document", timeDeltaPerDocument(len(docs), time.Since(startTime)))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return bulkDocsResponse, fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&bulkDocsResponse); err != nil {
		return bulkDocsResponse, err
	}

	return bulkDocsResponse, nil

}

func (s SGDataStore) BulkCreateDocumentsRetry(docs []Document, newEdits bool) ([]sgreplicate.DocumentRevisionPair, error) {

	totalPushedDocRevPairs := []sgreplicate.DocumentRevisionPair{}
	numRetries := 10
	sleepMsBetweenRetry := 500
	retrySleeper := CreateDoublingSleeperFunc(numRetries, sleepMsBetweenRetry)
	pendingDocs := docs[:]

	// Create retry worker that knows how to do actual work
	retryWorker := func() (shouldRetry bool, err error, value interface{}) {

		if len(pendingDocs) != len(docs) {
			logger.Debug("BulkCreateDocumentsRetry about to retry", "numdocs", len(pendingDocs))
		}

		pushedDocRevPairs, err := s.BulkCreateDocuments(pendingDocs, newEdits)

		// If any of the bulk docs had errors, remove them from the response.
		successful, failed := splitSucceededAndFailed(pushedDocRevPairs)
		if len(failed) > 0 {
			logger.Warn(
				"BulkCreateDocumentsRetry did not push all docs",
				"numdocs",
				len(docs),
				"numpushed",
				len(pushedDocRevPairs),
				"numsuccess",
				len(successful),
				"numerrors",
				len(failed),
			)
		}

		totalPushedDocRevPairs = append(totalPushedDocRevPairs, successful...)

		// Got all the docs, we're done
		if len(totalPushedDocRevPairs) == len(docs) {
			return false, nil, nil
		}

		// We need to retry the failed docs

		// Set pending docs to be the failed docs so that when we retry, we
		// only retry the failed docs
		pendingDocs = filterDocsIncluding(pendingDocs, failed)

		// Since the docs with errors will be retried, update retry stats
		s.StatsdClient.Counter(
			statsdSampleRate,
			"retries",
			len(failed),
		)

		return true, nil, nil

	}

	err, _ := RetryLoop(
		"BulkCreateDocumentsRetry",
		retryWorker,
		retrySleeper,
	)

	if len(totalPushedDocRevPairs) != len(docs) {
		panic(fmt.Sprintf("Unexpected number of docs pushed.  Got %v Expected %v", len(totalPushedDocRevPairs), len(docs)))
	}

	return totalPushedDocRevPairs, err

}

// Filter docs and only include the ones in docsToInclude
func filterDocsIncluding(docs []Document, docsToInclude []sgreplicate.DocumentRevisionPair) []Document {
	filteredSet := []Document{}
	for _, doc := range docs {
		shouldInclude := false
		for _, docToUpdateDocRevPair := range docsToInclude {
			if doc.Id() == docToUpdateDocRevPair.Id {
				shouldInclude = true
			}
		}
		if shouldInclude {
			filteredSet = append(filteredSet, doc)
		}
	}
	return filteredSet

}

func splitSucceededAndFailed(bulkDocsResponse []sgreplicate.DocumentRevisionPair) (successful []sgreplicate.DocumentRevisionPair, failed []sgreplicate.DocumentRevisionPair) {

	successful = []sgreplicate.DocumentRevisionPair{}
	failed = []sgreplicate.DocumentRevisionPair{}

	for _, docRevisionPair := range bulkDocsResponse {
		if docRevisionPair.Error != "" {
			failed = append(failed, docRevisionPair)
			logger.Warn("bulkDocsResponse contained errors", "docRevisionPair", docRevisionPair, "error", docRevisionPair.Error)
		} else {
			// no errors for this doc, add to result
			successful = append(successful, docRevisionPair)
		}
	}

	return successful, failed

}

func (s SGDataStore) addDocBodies(docs []Document) {
	for _, doc := range docs {
		docSizeBytes := doc["bodysize"].(int)
		doc["body"] = createBodyContentAsMapWithSize(docSizeBytes)
	}
}

func (s SGDataStore) BulkGetDocuments(r sgreplicate.BulkGetRequest) ([]sgreplicate.Document, error) {

	defer s.pushCounter("get_document_counter", len(r.Docs))

	bulkGetEndpoint, err := addEndpointToUrl(s.SyncGatewayUrl, "_bulk_get")
	if err != nil {
		return nil, err
	}

	bulkGetBytes, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("BulkGetDocuemnts failed to marshal request: %v", err)
	}

	buf := bytes.NewReader(bulkGetBytes)

	req, err := retryablehttp.NewRequest("POST", bulkGetEndpoint, buf)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")

	client := getHttpClient()

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	s.pushTimingStat("get_document", timeDeltaPerDocument(len(r.Docs), time.Since(startTime)))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return nil, fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	// Parse the response and make sure that we got all the docs we requested

	// Logger function that ignores any logging from the ReadBulkGetResponse method
	loggerFunc := func(key string, format string, args ...interface{}) {}
	documents, err := sgreplicate.ReadBulkGetResponse(resp, loggerFunc)
	if len(documents) != len(r.Docs) {
		return nil, fmt.Errorf("Expected %d docs, got %d docs", len(r.Docs), len(documents))
	}

	for _, doc := range documents {
		createAtRFC3339NanoIface, ok := doc.Body["created_at"]
		if !ok {
			logger.Warn("Document missing created_at field", "doc.Body", doc.Body)
			continue
		}
		createAtRFC3339NanoStr, ok := createAtRFC3339NanoIface.(string)
		if !ok {
			logger.Warn("Document created_at not a string", "doc.Body", doc.Body)
			continue
		}
		createAtRFC3339Nano, err := time.Parse(
			time.RFC3339Nano,
			createAtRFC3339NanoStr,
		)
		if err != nil {
			logger.Warn("Could not parse doc.created_at field into time", "createAtRFC3339Nano", createAtRFC3339Nano)
			continue
		}
		delta := time.Since(createAtRFC3339Nano)
		s.pushTimingStat("gateload_roundtrip", delta)

		possiblyLogVerboseWarning(delta, doc)

	}

	return documents, nil

}

// If the round trip time is over a certain threshold, log a verbose
// warning.  Trying to debug https://github.com/couchbaselabs/sgload/issues/12
func possiblyLogVerboseWarning(delta time.Duration, doc sgreplicate.Document) {

	maxSecondsExpected := time.Duration(60 * time.Second)
	if delta > time.Duration(time.Second*maxSecondsExpected) {

		createAtRFC3339NanoIface, ok := doc.Body["created_at"]
		if !ok {
			logger.Warn("Document missing created_at field", "doc.Body", doc.Body)
		}
		createAtRFC3339NanoStr, ok := createAtRFC3339NanoIface.(string)
		if !ok {
			logger.Warn("Document created_at not a string", "doc.Body", doc.Body)
		}
		createAtRFC3339Nano, err := time.Parse(
			time.RFC3339Nano,
			createAtRFC3339NanoStr,
		)
		if err != nil {
			logger.Warn("Could not parse doc.created_at field into time", "createAtRFC3339Nano", createAtRFC3339Nano)
		}

		logger.Warn(
			"Gateload roundtrip time exceeeded expected time",
			"maxSecondsExpected",
			maxSecondsExpected,
			"delta",
			delta,
			"createAtRFC3339NanoStr",
			createAtRFC3339NanoStr,
			"createAtRFC3339Nano",
			createAtRFC3339Nano,
			"time.Since(createAtRFC3339Nano)",
			time.Since(createAtRFC3339Nano),
			"now",
			time.Now(),
			"now RFC3339Nano",
			time.Now().Format(time.RFC3339Nano),
		)

	}

}

// add BasicAuth header for user if needed
func (s SGDataStore) addAuthIfNeeded(req *retryablehttp.Request) {
	if !s.UserCreds.Empty() {
		req.SetBasicAuth(s.UserCreds.Username, s.UserCreds.Password)
		req.Header.Set("X-sgload-username", s.UserCreds.Username)
	}
}

func (s SGDataStore) pushTimingStat(key string, delta time.Duration) {
	s.StatsdClient.Timing(
		statsdSampleRate,
		key,
		delta,
	)
}

func (s SGDataStore) pushCounter(key string, n int) {
	s.StatsdClient.Counter(
		statsdSampleRate,
		key,
		n,
	)
}

func splitHostPortWrapper(host string) (string, string, error) {
	if !strings.Contains(host, ":") {
		return host, "", nil
	}

	return net.SplitHostPort(host)
}

func addEndpointToUrl(urlStr, endpoint string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, endpoint)
	return u.String(), nil
}

func addTrailingSlash(urlStr string) string {
	return fmt.Sprintf("%v/", urlStr)
}

func timeDeltaPerDocument(numDocs int, timeDeltaAllDocs time.Duration) time.Duration {
	if numDocs == 0 {
		return timeDeltaAllDocs
	}
	return time.Duration(int64(timeDeltaAllDocs) / int64(numDocs))
}

func getHttpClient() *retryablehttp.Client {
	return sgClient
}

// Customize the DefaultTransport to have larger connection pool
// See http://tleyden.github.io/blog/2016/11/21/tuning-the-go-http-client-library-for-load-testing/
func transportWithConnPool(numConnections int) *http.Transport {

	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("http.DefaultTransport not an *http.Transport"))
	}
	customTransport := *defaultTransport
	customTransport.MaxIdleConns = numConnections
	customTransport.MaxIdleConnsPerHost = numConnections
	return &customTransport

}
