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
func createAndAssignDocs(agentIds []string, channelNames []string, numDocs, docSizeBytes int, docIdSuffix string) map[string][]Document {

	// Create Documents
	docsToWrite := createDocsToWrite(
		numDocs,
		docSizeBytes,
		docIdSuffix,
	)

	// Assign Docs to Channels (adds doc["channels"] field to each doc)
	docsToChannels := assignDocsToChannels(channelNames, docsToWrite)

	// Assign docs to writers, this returns a map keyed on writer which points
	// to doc slice for that writer
	docsToChannelsAndAgents := assignDocsToAgents(docsToChannels, agentIds)

	return docsToChannelsAndAgents

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

// Split the docs among the agents with an even distribution
func assignDocsToAgents(d []Document, agentIds []string) map[string][]Document {

	docAssignmentMapping := map[string][]Document{}
	for _, agentId := range agentIds {
		docAssignmentMapping[agentId] = []Document{}
	}

	for _, doc := range d {

		// choose a random agent
		agentIdIndex := rand.Intn(len(agentIds))
		agentId := agentIds[agentIdIndex]

		// add doc to writer's list of docs
		docsForAgent := docAssignmentMapping[agentId]
		docsForAgent = append(docsForAgent, doc)
		docAssignmentMapping[agentId] = docsForAgent

	}

	return docAssignmentMapping

}

func createDocsToWrite(numDocs, docSizeBytes int, docIdSuffix string) []Document {

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < numDocs; docNum++ {
		d = map[string]interface{}{}
		if docIdSuffix != "" {
			d["_id"] = fmt.Sprintf("%d-%s", docNum, docIdSuffix)
		}
		d["docNum"] = docNum
		d["body"] = createBodyContentAsMapWithSize(docSizeBytes)
		d["created_at"] = time.Now().Format(time.RFC3339Nano)
		docs = append(docs, d)
	}
	return docs

}

// Break things into batches, for example:
//
// batchSize: 3
// things: [t1, t2, t3, t4, t5]
//
// result:
//
//   [
//     [t1, t2, t3],  <-- batch 1
//     [t4, t5],      <-- batch 2 (incomplete, not enough to fill batch)
//
//   ]
func breakIntoBatches(batchSize int, docs []Document) [][]Document {

	batches := [][]Document{}

	numBatches := len(docs) / batchSize

	// is there residue?  if so, add one more to batch
	if len(docs)%batchSize != 0 {
		numBatches += 1
	}

	for i := 0; i < numBatches; i++ {
		batch := []Document{}
		for j := 0; j < batchSize; j++ {
			docIndex := i*batchSize + j
			if docIndex >= len(docs) {
				break
			}
			doc := docs[docIndex]
			batch = append(batch, doc)
		}
		batches = append(batches, batch)
	}

	return batches

}
