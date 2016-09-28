package sgload

import "testing"

func TestGetDocsReadyToUpdateLessThanBatch(t *testing.T) {

	batchSize := 2
	maxUpdatesPerDoc := 100

	docUpdateStatuses := map[string]DocUpdateStatus{
		"doc-1": DocUpdateStatus{
			NumUpdates: 10,
			LatestRev:  "100-blah",
		},
	}
	docsReadyToUpdate := getDocsReadyToUpdate(
		batchSize,
		maxUpdatesPerDoc,
		docUpdateStatuses,
	)

	if len(docsReadyToUpdate) != 1 {
		t.Errorf("Expected 1 doc ready to update, got 0")
	}

}

func TestGetDocsReadyToUpdateMoreThanBatch(t *testing.T) {

	batchSize := 2
	maxUpdatesPerDoc := 100

	docUpdateStatuses := map[string]DocUpdateStatus{
		"doc-1": DocUpdateStatus{
			NumUpdates: 10,
			LatestRev:  "100-blah",
		},
		"doc-2": DocUpdateStatus{
			NumUpdates: 100,
			LatestRev:  "100-blah",
		},
		"doc-3": DocUpdateStatus{
			NumUpdates: 1,
			LatestRev:  "1-blah",
		},
		"doc-4": DocUpdateStatus{
			NumUpdates: 2,
			LatestRev:  "2-blah",
		},
	}
	docsReadyToUpdate := getDocsReadyToUpdate(
		batchSize,
		maxUpdatesPerDoc,
		docUpdateStatuses,
	)

	if len(docsReadyToUpdate) != batchSize {
		t.Errorf("Expected batchsize docs ready to update, got something else")
	}

}
