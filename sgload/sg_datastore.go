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
	"regexp"
	"strings"
)

type SGDataStore struct {
	MaxConcurrentHttpClients chan struct{} // TODO: currenty ignored.  TODO: REMOVE!
	SyncGatewayUrl           string
	SyncGatewayAdminPort     int
	UserCreds                UserCred
}

func NewSGDataStore(sgUrl string, sgAdminPort int, maxConcurrentHttpClients chan struct{}) *SGDataStore {
	return &SGDataStore{
		MaxConcurrentHttpClients: maxConcurrentHttpClients,
		SyncGatewayUrl:           sgUrl,
		SyncGatewayAdminPort:     sgAdminPort,
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
	adminUrlUserEndpoint := fmt.Sprintf("%v/_user/", adminUrl)

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

	log.Printf("Parsing url: %v", s.SyncGatewayUrl)
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
	log.Printf("host: %v port: %v", host, port)

	if port != "" {
		// is there a port?
		// do a regex replace on :port on the original url
		r := regexp.MustCompile(port)
		log.Printf("Replace %v with %v in %v", port, s.SyncGatewayAdminPort, sgAdminURL)
		sgAdminURL = r.ReplaceAllString(
			sgAdminURL,
			fmt.Sprintf("%d", s.SyncGatewayAdminPort),
		)

	} else {
		// is there no port?
		// do a regex replace of host with host:port
		r := regexp.MustCompile(host)
		hostWithAdminPort := fmt.Sprintf("%v:%v", host, s.SyncGatewayAdminPort)
		log.Printf("Replace %v with %v in %v", host, hostWithAdminPort, sgAdminURL)
		sgAdminURL = r.ReplaceAllString(
			sgAdminURL,
			hostWithAdminPort,
		)

	}

	// return it
	return sgAdminURL, nil

}

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

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return nil
}

// add BasicAuth header for user if needed
func (s SGDataStore) addAuthIfNeeded(req *http.Request) {

	if !s.UserCreds.Empty() {
		log.Printf("set basic auth to %+v", s.UserCreds)
		req.SetBasicAuth(s.UserCreds.Username, s.UserCreds.Password)
	} else {
		log.Printf("not adding basic auth header, no user creds: %+v", s)
	}
}

func splitHostPortWrapper(host string) (string, string, error) {
	if !strings.Contains(host, ":") {
		return host, "", nil
	}

	return net.SplitHostPort(host)
}
