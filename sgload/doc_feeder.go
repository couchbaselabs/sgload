package sgload

import (
	"fmt"
	"math/rand"
	"time"
)

// Assign docs to channels w/ equal distribution of docs into channels.
// Assign docs to writers with an equal distribution of docs into writers,
// but mix up so each writer is writing to a variety of channels.
// This returns a map keyed on writer which points to doc slice for that writer
func createAndAssignDocs(writers []*Writer, channelNames []string, numDocs, docSizeBytes int) map[*Writer][]Document {

	// Create Documents
	docsToWrite := createDocsToWrite(
		numDocs,
		docSizeBytes,
	)

	// Assign Docs to Channels (adds doc["channels"] field to each doc)
	docsToChannels := assignDocsToChannels(channelNames, docsToWrite)

	// Assign docs to writers, this returns a map keyed on writer which points
	// to doc slice for that writer
	docsToChannelsAndWriters := assignDocsToWriters(docsToChannels, writers)

	return docsToChannelsAndWriters

}

// Assigns docs to channels with as even of a distribution as possible.
func assignDocsToChannels(channelNames []string, inputDocs []Document) []Document {

	docs := []Document{}

	if len(channelNames) > len(inputDocs) {
		panic(fmt.Sprintf("Num chans (%d) must be LTE to num docs (%d)", len(channelNames), len(inputDocs)))
	}
	if len(channelNames) == 0 {
		panic(fmt.Sprintf("Cannot call assignDocsToChannels with empty channelNames"))
	}

	for docNum, inputDoc := range inputDocs {
		chanIndex := docNum % len(channelNames)
		channelName := channelNames[chanIndex]
		inputDoc["channels"] = []string{channelName}
		docs = append(docs, inputDoc)
	}

	return docs

}

// Split the docs among the writers with an even distribution
func assignDocsToWriters(d []Document, writers []*Writer) map[*Writer][]Document {

	docAssignmentMapping := map[*Writer][]Document{}
	for _, writer := range writers {
		docAssignmentMapping[writer] = []Document{}
	}

	for _, doc := range d {

		// choose a random writer
		writerIndex := rand.Intn(len(writers))

		writer := writers[writerIndex]

		// add doc to writer's list of docs
		docsForWriter := docAssignmentMapping[writer]
		docsForWriter = append(docsForWriter, doc)
		docAssignmentMapping[writer] = docsForWriter

	}

	return docAssignmentMapping

}

func createDocsToWrite(numDocs, docSizeBytes int) []Document {

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < numDocs; docNum++ {
		d = map[string]interface{}{}
		d["docNum"] = docNum
		d["body"] = createBodyContentWithSize(docSizeBytes)
		d["created_at"] = time.Now().Format(time.RFC3339Nano)
		docs = append(docs, d)
	}
	return docs

}
