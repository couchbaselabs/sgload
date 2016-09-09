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
)

type SGDataStore struct {
	SyncGatewayUrl       string
	SyncGatewayAdminPort int
	UserCreds            UserCred
}

func NewSGDataStore(sgUrl string, sgAdminPort int) *SGDataStore {
	return &SGDataStore{
		SyncGatewayUrl:       sgUrl,
		SyncGatewayAdminPort: sgAdminPort,
	}
}

func (s *SGDataStore) SetUserCreds(u UserCred) {
	s.UserCreds = u
}

func (s SGDataStore) CreateUser(u UserCred) error {

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
	userDoc["admin_channels"] = []string{"*"}

	docBytes, err := json.Marshal(userDoc)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(docBytes)

	req, err := http.NewRequest("POST", adminUrlUserEndpoint, buf)

	req.Header.Set("Content-Type", "application/json")

	client := http.DefaultClient

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 201 {
		return fmt.Errorf("Unexpected response status for POST request: %d", resp.StatusCode)
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

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

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
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
		NewEdits:  false,
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

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
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

// add BasicAuth header for user if needed
func (s SGDataStore) addAuthIfNeeded(req *http.Request) {
	if !s.UserCreds.Empty() {
		req.SetBasicAuth(s.UserCreds.Username, s.UserCreds.Password)
	}
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
