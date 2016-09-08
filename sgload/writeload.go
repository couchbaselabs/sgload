package sgload

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/satori/go.uuid"
)

type WriteLoadSpec struct {
	LoadSpec
	NumWriters               int
	NumChannels              int
	DocSizeBytes             int
	NumDocs                  int
	MaxConcurrentHttpClients int
}

func (wls WriteLoadSpec) Validate() error {
	if wls.NumWriters <= 0 {
		return fmt.Errorf("NumWriters must be greater than zero")
	}

	if !wls.CreateUsers {
		userCreds, err := wls.loadUserCredsFromArgs()
		if err != nil {
			return err
		}
		if len(userCreds) != wls.NumWriters {
			return fmt.Errorf("You only provided %d user credentials, but specified %d writers", len(userCreds), wls.NumWriters)
		}
	}

	if wls.NumChannels > wls.NumDocs {
		return fmt.Errorf("Number of channels must be less than or equal to number of docs")
	}

	if err := wls.LoadSpec.Validate(); err != nil {
		return err
	}
	return nil
}

// Validate this spec or panic
func (wls WriteLoadSpec) MustValidate() {
	if err := wls.Validate(); err != nil {
		log.Panicf("Invalid WriteLoadSpec: %+v. Error: %v", wls, err)
	}
}

type WriteLoadRunner struct {
	WriteLoadSpec          WriteLoadSpec
	MaxHttpClientSemaphore chan struct{}
}

func NewWriteLoadRunner(wls WriteLoadSpec) *WriteLoadRunner {
	wls.MustValidate()

	// Create a count semaphore so that only MaxConcurrentHttpClients can be active at any given time
	mhcs := make(chan struct{}, wls.MaxConcurrentHttpClients)

	return &WriteLoadRunner{
		WriteLoadSpec:          wls,
		MaxHttpClientSemaphore: mhcs,
	}
}

func (wlr WriteLoadRunner) Run() error {

	var wg sync.WaitGroup

	// Create writers
	writers, err := wlr.createWriters(&wg)
	if err != nil {
		return err
	}
	for _, writer := range writers {
		go writer.Run()
	}

	go wlr.feedDocsToWriters(writers)

	log.Printf("Waiting for writers to finish")
	wg.Wait()
	log.Printf("Writers finished")

	return nil
}

func (wlr WriteLoadRunner) createDataStore() DataStore {

	if wlr.WriteLoadSpec.MockDataStore {
		return NewMockDataStore(wlr.MaxHttpClientSemaphore)
	}

	return NewSGDataStore(
		wlr.WriteLoadSpec.SyncGatewayUrl,
		wlr.MaxHttpClientSemaphore,
	)

}

func (wlr WriteLoadRunner) createWriters(wg *sync.WaitGroup) ([]*Writer, error) {

	writers := []*Writer{}
	var userCreds []UserCred
	var err error

	switch wlr.WriteLoadSpec.CreateUsers {
	case true:
		userCreds = wlr.generateUserCreds()
	default:
		userCreds, err = wlr.WriteLoadSpec.loadUserCredsFromArgs()
		if err != nil {
			return writers, err
		}
	}

	for userId := 0; userId < wlr.WriteLoadSpec.NumWriters; userId++ {
		userCred := userCreds[userId]

		writer := NewWriter(wg, userId, userCred, wlr.createDataStore())
		writer.CreateDataStoreUser = wlr.WriteLoadSpec.CreateUsers
		writers = append(writers, writer)
		wg.Add(1)
	}

	return writers, nil

}

func (wlr WriteLoadRunner) generateUserCreds() []UserCred {
	userCreds := []UserCred{}
	for userId := 0; userId < wlr.WriteLoadSpec.NumWriters; userId++ {
		uuid := NewUuid()
		userCred := UserCred{
			Username: fmt.Sprintf("writeload-user-%d-%s", userId, uuid),
			Password: fmt.Sprintf("writeload-passw0rd-%d-%s", userId, uuid),
		}
		userCreds = append(userCreds, userCred)

	}
	return userCreds

}

func (wlr WriteLoadRunner) feedDocsToWriters(writers []*Writer) error {

	docsToWrite := wlr.createDocsToWrite()
	docsToWrite = wlr.assignDocsToChannels(docsToWrite)
	docAssignmentMapping := wlr.assignDocsToWriters(docsToWrite, writers)

	for writer, docsToWrite := range docAssignmentMapping {
		writer.AddToDataStore(docsToWrite)
	}

	for _, writer := range writers {
		d := Document{}
		d["_terminal"] = true
		writer.AddToDataStore([]Document{d})
	}

	return nil

}

func (wlr WriteLoadRunner) assignDocsToChannels(inputDocs []Document) []Document {

	docs := []Document{}
	channelNames := wlr.generateChannelNames()

	if len(channelNames) > len(inputDocs) {
		log.Panicf("Number of channels must be less than or equal to number of docs")
	}

	for docNum, inputDoc := range inputDocs {
		chanIndex := docNum % len(channelNames)
		channelName := channelNames[chanIndex]
		inputDoc["channels"] = []string{channelName}
		docs = append(docs, inputDoc)
	}

	return docs

}

func (wlr WriteLoadRunner) generateChannelNames() []string {
	channelNames := []string{}
	for i := 0; i < wlr.WriteLoadSpec.NumChannels; i++ {
		channelNames = append(channelNames, fmt.Sprintf("%d", i))
	}
	return channelNames
}

func (wlr WriteLoadRunner) createDocsToWrite() []Document {

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < wlr.WriteLoadSpec.NumDocs; docNum++ {
		d = map[string]interface{}{}
		d["docNum"] = docNum
		d["body"] = createBodyContentWithSize(wlr.WriteLoadSpec.DocSizeBytes)
		docs = append(docs, d)
	}
	return docs

}

func (wlr WriteLoadRunner) assignDocsToWriters(d []Document, w []*Writer) map[*Writer][]Document {

	docAssignmentMapping := map[*Writer][]Document{}
	for _, writer := range w {
		docAssignmentMapping[writer] = []Document{}
	}

	for docNum, doc := range d {

		// figure out which writer to assign this doc to
		writerIndex := docNum % len(w)
		writer := w[writerIndex]

		// add doc to writer's list of docs
		docsForWriter := docAssignmentMapping[writer]
		docsForWriter = append(docsForWriter, doc)
		docAssignmentMapping[writer] = docsForWriter

	}

	return docAssignmentMapping

}

func createBodyContentWithSize(docSizeBytes int) string {
	buf := bytes.Buffer{}
	for i := 0; i < docSizeBytes; i++ {
		buf.WriteString("a")
	}
	return buf.String()
}

func NewUuid() string {
	u4 := uuid.NewV4()
	return fmt.Sprintf("%s", u4)
}
