package sgload

import (
	"fmt"
	"time"

	"github.com/couchbaselabs/sg-replicate"
)

type DocumentMetadata struct {
	sgreplicate.DocumentRevisionPair
	Channels []string
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

func createDocsToWrite(writerUsername string, docIdOffset, numDocs, docSizeBytes int, docIdSuffix string) []Document {

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < numDocs; docNum++ {
		globalDocNum := docIdOffset + docNum
		d = map[string]interface{}{}
		if docIdSuffix != "" {
			d["_id"] = fmt.Sprintf("%d-%s", globalDocNum, writerUsername)
		}
		d["docNum"] = globalDocNum // <-- needed?
		d["bodysize"] = docSizeBytes
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

// Break things into batches, for example:
//
// batchSize: 3
// totalNum: 5
//
// result:
//
//   [
//     3,  <-- batch 1, size 3
//     2,  <-- batch 2, size 2 (incomplete, not enough to fill batch)
//
//   ]
func breakIntoBatchesCount(batchSize int, totalNum int) (batches []int) {

	batches = []int{}

	numBatches := totalNum / batchSize

	// is there residue?  if so, add one more to batch
	if totalNum%batchSize != 0 {
		numBatches += 1
	}

	for i := 0; i < numBatches; i++ {
		batch := 0
		for j := 0; j < batchSize; j++ {
			index := i*batchSize + j
			if index >= totalNum {
				break
			}
			batch += 1
		}
		batches = append(batches, batch)
	}

	return batches

}
