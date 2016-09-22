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
