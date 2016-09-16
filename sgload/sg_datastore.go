package sgload

import (
	"bytes"
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
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
	"github.com/peterbourgon/g2s"
)

var (
	// The sample rate -- set this to 0.1 if you only want
	// 10% of the samples to be pushed to statds, or .01 if you only
	// want 1% of the samples pushed to statsd.  Useful for
	// not overwhelming stats if you have too many samples.
	statsdSampleRate float32 = 1.0
)

type SGDataStore struct {
	SyncGatewayUrl       string
	SyncGatewayAdminPort int
	UserCreds            UserCred
	StatsdClient         *g2s.Statsd
}

func NewSGDataStore(sgUrl string, sgAdminPort int, statsdClient *g2s.Statsd) *SGDataStore {
	return &SGDataStore{
		SyncGatewayUrl:       sgUrl,
		SyncGatewayAdminPort: sgAdminPort,
		StatsdClient:         statsdClient,
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
	buf := bytes.NewBuffer(docBytes)

	req, err := http.NewRequest("POST", adminUrlUserEndpoint, buf)

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	s.pushTimingStat("create_user", time.Since(startTime))

	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
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

	req, err := http.NewRequest("GET", changesFeedUrl, nil)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return sgreplicate.Changes{}, sinceVal, err
	}
	s.pushTimingStat("changes_feed", time.Since(startTime))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return sgreplicate.Changes{}, sinceVal, fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
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
func (s SGDataStore) CreateDocument(d Document) error {

	docBytes, err := json.Marshal(d)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(docBytes)

	req, err := http.NewRequest("POST", s.SyncGatewayUrl, buf)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	s.pushTimingStat("create_document", time.Since(startTime))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)

	return nil
}

// Bulk create a set of documents in Sync Gateway
func (s SGDataStore) BulkCreateDocuments(docs []Document) error {

	bulkDocsEndpoint, err := addEndpointToUrl(s.SyncGatewayUrl, "_bulk_docs")
	if err != nil {
		return err
	}

	bulkDocs := BulkDocs{
		Documents: docs,
		NewEdits:  true,
	}
	docBytes, err := json.Marshal(bulkDocs)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(docBytes)

	req, err := http.NewRequest("POST", bulkDocsEndpoint, buf)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	s.pushTimingStat("create_document", timeDeltaPerDocument(len(docs), time.Since(startTime)))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	bulkDocsResponse := []DocumentRevisionPair{}
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&bulkDocsResponse); err != nil {
		return err
	}

	// If any of the bulk docs had errors, return an error
	for _, docRevisionPair := range bulkDocsResponse {
		if docRevisionPair.Error != "" {
			return fmt.Errorf("%v", docRevisionPair.Error)
		}
	}

	return nil

}

func (s SGDataStore) BulkGetDocuments(r sgreplicate.BulkGetRequest) error {

	// TODO: needs to check response status of each doc to
	// make sure no errors pulling docs

	bulkGetEndpoint, err := addEndpointToUrl(s.SyncGatewayUrl, "_bulk_get")
	if err != nil {
		return err
	}

	bulkGetBytes, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("BulkGetDocuemnts failed to marshal request: %v", err)
	}

	buf := bytes.NewBuffer(bulkGetBytes)

	req, err := http.NewRequest("POST", bulkGetEndpoint, buf)
	s.addAuthIfNeeded(req)

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient

	startTime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	s.pushTimingStat("get_document", timeDeltaPerDocument(len(r.Docs), time.Since(startTime)))
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)

	return nil

}

// add BasicAuth header for user if needed
func (s SGDataStore) addAuthIfNeeded(req *http.Request) {
	if !s.UserCreds.Empty() {
		req.SetBasicAuth(s.UserCreds.Username, s.UserCreds.Password)
	}
}

func (s SGDataStore) pushTimingStat(key string, delta time.Duration) {
	if s.StatsdClient == nil {
		return
	}
	s.StatsdClient.Timing(
		statsdSampleRate,
		key,
		delta,
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
	return time.Duration(int64(timeDeltaAllDocs) / int64(numDocs))
}
