package sgload

import (
	"fmt"
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
