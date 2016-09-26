package sgload

import (
	"fmt"
	"log"
	"testing"
)

func TestCreateAndAssignDocs(t *testing.T) {

	numDocs := 49
	docByteSize := 1
	numWriters := 10
	numChannels := 10

	writers := generateWriters(numWriters)
	channelNames := generateChannels(numChannels)

	docsToChannelsAndWriters := createAndAssignDocs(
		writers,
		channelNames,
		numDocs,
		docByteSize,
	)

	if len(docsToChannelsAndWriters) != len(writers) {
		t.Errorf("Expected map to have each writer as keys")
	}

	uniqueChannelsFromDocs := map[string]struct{}{}
	for writer, docs := range docsToChannelsAndWriters {
		log.Printf("Docs/channels assigned to writer %d", writer.ID)
		uniqueChannelsPerWriter := map[string]struct{}{}
		for _, doc := range docs {
			channelNames := doc.channelNames()
			for _, channelName := range channelNames {
				uniqueChannelsFromDocs[channelName] = struct{}{}
				uniqueChannelsPerWriter[channelName] = struct{}{}
			}
			log.Printf("doc id: %v, channels: %v", doc["docNum"], channelNames)
		}
		log.Printf("uniqueChannelsPerWriter: %v", uniqueChannelsPerWriter)
		// if things are "mixed up" properly, then writers should be
		// getting a nice distribution of channels, and approx half of
		// their docs should be in unique channels
		if len(docs) >= 2 {
			halfNumDocs := (len(docs) / 2)
			if len(uniqueChannelsPerWriter) < halfNumDocs {
				t.Errorf("Expected more unique chans per writer.  Got %d, expected %d", len(uniqueChannelsPerWriter), halfNumDocs)
			}
		}
	}

	if len(uniqueChannelsFromDocs) != len(channelNames) {
		t.Errorf("Expected to see all channels, got: %+v", uniqueChannelsFromDocs)
	}

}

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

func generateWriters(numWriters int) []*Writer {
	writers := []*Writer{}
	for i := 0; i < numWriters; i++ {
		writers = append(writers, &Writer{Agent: Agent{ID: i}})
	}
	return writers
}

func generateChannels(numChannels int) []string {
	channelNames := []string{}
	for i := 0; i < numChannels; i++ {
		channelNames = append(channelNames, fmt.Sprintf("%d", i))
	}
	return channelNames
}
