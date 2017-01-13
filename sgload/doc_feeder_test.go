package sgload

import (
	"fmt"
	"log"
	"testing"
)

func TestBreakIntoBatches(t *testing.T) {

	batchSize := 3
	things := []Document{
		Document{},
		Document{},
		Document{},
		Document{},
		Document{},
	}

	batches := breakIntoBatches(batchSize, things)
	if len(batches) != 2 {
		t.Fatalf("Expecting 2 batches")
	}
	batch1 := batches[0]
	if len(batch1) != batchSize {
		t.Fatalf("Expecting batch1 to be batchsize")
	}

	batch2 := batches[1]
	if len(batch2) != 2 {
		t.Fatalf("Expecting batch2 to have two items")
	}

}

func TestBreakIntoBatchesOversizedBatch(t *testing.T) {

	batchSize := 10
	things := []Document{
		Document{},
		Document{},
		Document{},
		Document{},
		Document{},
	}

	batches := breakIntoBatches(batchSize, things)
	if len(batches) != 1 {
		t.Fatalf("Expecting 1 batches")
	}
	batch1 := batches[0]
	if len(batch1) != len(things) {
		t.Fatalf("Expecting batch1 to be len(things)")
	}

}

func TestBreakIntoBatchesCount(t *testing.T) {

	batchSize := 3
	totalNum := 5

	batches := breakIntoBatchesCount(batchSize, totalNum)
	if len(batches) != 2 {
		t.Fatalf("Expecting 2 batches")
	}
	batch1 := batches[0]
	if batch1 != batchSize {
		t.Fatalf("Expecting batch1 to be batchsize")
	}

	batch2 := batches[1]
	if batch2 != 2 {
		t.Fatalf("Expecting batch2 to have two items")
	}

}

func TestBreakIntoBatchesZeroBatchSize(t *testing.T) {
	batchSize := 0
	totalNum := 5

	batches := breakIntoBatchesCount(batchSize, totalNum)
	if len(batches) != 5 {
		t.Fatalf("Expecting 5 batches")
	}
	batch1 := batches[0]
	if batch1 != 1 {
		t.Fatalf("Expecting batch1 to be 1")
	}
}

func TestFeedDocsToWriter(t *testing.T) {

	writer := Writer{}
	writeLoadSpec := WriteLoadSpec{}

	docsPerWriter := 100
	writer.BatchSize = 10 // TODO: test with smaller batch size
	docBatches := breakIntoBatchesCount(writer.BatchSize, docsPerWriter)

	// Make an outbound docs channel big enough to hold all the batches
	bufChanSize := len(docBatches)
	bufChanSize += 1 // terminal doc
	writer.OutboundDocs = make(chan []Document, bufChanSize)

	channelNames := []string{"ABC", "CBS"}
	err := feedDocsToWriter(
		&writer,
		writeLoadSpec,
		docsPerWriter,
		channelNames,
	)
	if err != nil {
		t.Fatalf("Got error trying to call feedDocsToWriter: %v", err)
	}

	for i := 0; i < len(docBatches); i++ {
		docSlice := <-writer.OutboundDocs
		log.Printf("Got docSlice %d from channel: %v", i, docSlice)
		if len(docSlice) != writer.BatchSize {
			t.Fatalf("Got unexpected doc slice size")
		}
	}

}

func TestGetChannelToDocMapping(t *testing.T) {
	numDocs := 100
	channelNames := []string{
		"ABC",
		"CBS",
		"CNN",
		"NBC",
		"FOX",
	}
	channelToDocMapping := getChannelToDocMapping(numDocs, channelNames)
	if len(channelToDocMapping) != numDocs {
		t.Fatalf("Got unexpected len of channelToDocMapping")
	}
	numAbc := 0
	numCbs := 0
	numCnn := 0
	numNbc := 0
	numFox := 0

	for _, chanIndex := range channelToDocMapping {
		// log.Printf("chanIndex: %d", chanIndex)
		switch chanIndex {
		case 0:
			numAbc += 1
		case 1:
			numCbs += 1
		case 2:
			numCnn += 1
		case 3:
			numNbc += 1
		case 4:
			numFox += 1
		}
	}
	if numAbc == 0 || numCbs == 0 || numCnn == 0 {
		t.Fatalf("Unexpected channel distribution")
	}

	log.Printf("numAbc: %d numCbs: %d numCnn: %d numNbc: %d numFox: %d", numAbc, numCbs, numCnn, numNbc, numFox)

	if absolute(numAbc-numCbs) > 0 {
		t.Fatalf("Unexpected channel distribution")
	}
	if absolute(numCbs-numCnn) > 0 {
		t.Fatalf("Unexpected channel distribution")
	}
	if absolute(numCnn-numFox) > 0 {
		t.Fatalf("Unexpected channel distribution")
	}

}

func generateAgentIds(numAgents int) []string {
	agentIds := []string{}
	for i := 0; i < numAgents; i++ {
		agentIds = append(agentIds, fmt.Sprintf("agent-%d", i))
	}
	return agentIds
}

func generateChannels(numChannels int) []string {
	channelNames := []string{}
	for i := 0; i < numChannels; i++ {
		channelNames = append(channelNames, fmt.Sprintf("%d", i))
	}
	return channelNames
}

func absolute(n int) int {
	returnVal := n
	if n < 0 {
		returnVal = -returnVal
	}
	return returnVal
}
