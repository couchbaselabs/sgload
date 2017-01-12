package sgload

import (
	"fmt"
	"time"

	sgreplicate "github.com/couchbaselabs/sg-replicate"
)

type DocumentMetadata struct {
	sgreplicate.DocumentRevisionPair
	Channels []string
}

// Assigns docs to channels with as even of a distribution as possible.
func assignDocsToChannels(docsToWrite []Document, channelToDocMapping []uint16, channelNames []string) {

	for _, doc := range docsToWrite {
		perWriterDocCounter := doc["per_writer_doc_counter"].(int)
		channelIndex := channelToDocMapping[perWriterDocCounter]
		channelName := channelNames[channelIndex]
		doc["channels"] = []string{channelName}
	}

}

func createDocsToWrite(writerUsername string, docIdOffset, numDocs, docSizeBytes int, docIdSuffix string) []Document {

	var d Document
	docs := []Document{}

	for docNum := 0; docNum < numDocs; docNum++ {

		// This will be a monotonically incrementing doc counter that is per-writer
		perWriterDocCounter := docIdOffset + docNum

		d = map[string]interface{}{}
		// Create a unique document id
		if docIdSuffix != "" {
			d["_id"] = fmt.Sprintf("%d-%s", perWriterDocCounter, writerUsername)
		}
		d["per_writer_doc_counter"] = perWriterDocCounter
		d["bodysize"] = docSizeBytes
		d["created_at"] = time.Now().Format(time.RFC3339Nano)
		docs = append(docs, Document(d))
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

	// Take care of special edge case -- if batchSize is 0, which is
	// impossible, then round it up to smallest valid batch size of 1.
	// Fixes divide by 0 error.
	if batchSize == 0 {
		batchSize = 1
	}

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

func feedDocsToWriter(writer *Writer, wls WriteLoadSpec, approxDocsPerWriter int, channelNames []string) error {

	logger.Debug("Feeding docs to writer", "writer", writer.UserCred.Username)

	channelToDocMapping := getChannelToDocMapping(approxDocsPerWriter, channelNames)

	docIdOffset := 0

	// loop over approxDocsPerWriter and push batchSize docs until
	// no more docs left to push
	docBatches := breakIntoBatchesCount(writer.BatchSize, approxDocsPerWriter)
	for _, docBatch := range docBatches {

		// Create Documents
		docsToWrite := createDocsToWrite(
			writer.UserCred.Username,
			docIdOffset,
			docBatch,
			wls.DocSizeBytes,
			wls.TestSessionID,
		)

		// Assign Docs to Channels (adds doc["channels"] field to each doc)
		assignDocsToChannels(
			docsToWrite,
			channelToDocMapping,
			channelNames,
		)

		writer.AddToDataStore(docsToWrite)

		docIdOffset += docBatch

	}

	// Send terminal docs which will shutdown writers after they've
	// processed all the normal docs
	logger.Debug("Feeding terminal doc to writer", "writer", writer.Agent.UserCred.Username)
	d := Document{}
	d["_terminal"] = true
	writer.AddToDataStore([]Document{d})

	return nil

}

func getChannelToDocMapping(approxDocsPerWriter int, channelNames []string) []uint16 {

	// Prevent integer overflow on the uint16 based channel indexes
	if len(channelNames) > 65535 {
		panic(fmt.Sprintf("Does not support this many channels"))
	}

	// Make sure number docs divide into channels evenly
	remainder := approxDocsPerWriter % len(channelNames)
	if remainder != 0 {
		panic(fmt.Sprintf("Numdocs (%d) does not divide into num channels evenly (%d)", approxDocsPerWriter, len(channelNames)))
	}

	channelToDocMapping := make([]uint16, approxDocsPerWriter)

	for docIndex := 0; docIndex < len(channelToDocMapping); docIndex += 1 {

		// old way, was too predictable
		chanIndex := docIndex % len(channelNames)

		// chanIndex := rand.Intn(len(channelNames))

		channelToDocMapping[docIndex] = uint16(chanIndex)

	}

	return channelToDocMapping

}
