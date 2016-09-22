package sgload

import (
	"log"
	"testing"
)

func TestCreateAndAssignDocs(t *testing.T) {

	numDocs := 49
	docByteSize := 1

	writers := []*Writer{
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
		&Writer{},
	}

	channelNames := []string{
		"1",
		"2",
		"3",
		"4",
		"5",
		"6",
		"7",
		"8",
		"9",
		"10",
	}
	docsToChannelsAndWriters := createAndAssignDocs(
		writers,
		channelNames,
		numDocs,
		docByteSize,
	)

	atLeastnumDocsPerWriter := numDocs / len(writers)
	log.Printf("atLeastnumDocsPerWriter: %v", atLeastnumDocsPerWriter)

	log.Printf("docsToChannelsAndWriters: %+v", docsToChannelsAndWriters)
	if len(docsToChannelsAndWriters) != len(writers) {
		t.Errorf("Expected map to have each writer as keys")
	}

	uniqueChannelsFromDocs := map[string]struct{}{}
	for _, docs := range docsToChannelsAndWriters {
		uniqueChannelsPerWriter := map[string]struct{}{}
		if len(docs) < atLeastnumDocsPerWriter {
			t.Errorf("Expected len(docs) (%v) >= atLeastnumDocsPerWriter (%v)", len(docs), atLeastnumDocsPerWriter)
		}
		for _, doc := range docs {
			channelNames := doc.channelNames()
			for _, channelName := range channelNames {
				uniqueChannelsFromDocs[channelName] = struct{}{}
				uniqueChannelsPerWriter[channelName] = struct{}{}
			}
		}
		log.Printf("uniqueChannelsPerWriter: %v", uniqueChannelsPerWriter)
		// if things are "mixed up" properly, then writers should be
		// getting a nice distribution of channels, and approx half of
		// their docs should be in unique channels
		halfNumDocs := (len(docs) / 2)
		if len(uniqueChannelsPerWriter) < halfNumDocs {
			t.Errorf("Expected more unique chans per writer.  Got %d, expected %d", len(uniqueChannelsPerWriter), halfNumDocs)
		}
	}

	if len(uniqueChannelsFromDocs) != len(channelNames) {
		t.Errorf("Expected to see all channels, got: %+v", uniqueChannelsFromDocs)
	}

}
