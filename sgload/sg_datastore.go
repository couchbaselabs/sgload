package sgload

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
)

type SGDataStore struct {
	MaxConcurrentHttpClients chan struct{} // TODO: currenty ignored
	SyncGatewayUrl           string
}

func NewSGDataStore(sgUrl string, maxConcurrentHttpClients chan struct{}) *SGDataStore {

	return &SGDataStore{
		MaxConcurrentHttpClients: maxConcurrentHttpClients,
		SyncGatewayUrl:           sgUrl,
	}
}

func (s SGDataStore) CreateUser(u UserCred) error {

	return nil
}

func (s SGDataStore) CreateDocument(d Document) error {

	// TODO: need to add BasicAuth header for user?

	docBytes, err := json.Marshal(d)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(docBytes)
	resp, err := http.Post(s.SyncGatewayUrl, "application/json", buf)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return nil
}