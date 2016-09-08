package sgload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

type SGDataStore struct {
	MaxConcurrentHttpClients chan struct{} // TODO: currenty ignored
	SyncGatewayUrl           string
	UserCreds                UserCred
}

func NewSGDataStore(sgUrl string, maxConcurrentHttpClients chan struct{}) *SGDataStore {

	return &SGDataStore{
		MaxConcurrentHttpClients: maxConcurrentHttpClients,
		SyncGatewayUrl:           sgUrl,
	}
}

func (s *SGDataStore) SetUserCreds(u UserCred) {
	s.UserCreds = u
}

func (s SGDataStore) CreateUser(u UserCred) error {

	return nil
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
