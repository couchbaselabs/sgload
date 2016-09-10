package sgload

import "testing"

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
