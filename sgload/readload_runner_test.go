package sgload

import "testing"

func TestAssignChannelsToReader(t *testing.T) {
	numChansPerReader := 2
	sgChannels := []string{
		"1",
		"2",
		"3",
		"4",
		"5",
		"6",
	}

	uniqueChansTotal := map[string]interface{}{}

	for i := 0; i < 100; i++ {

		uniqueChans := map[string]interface{}{}
		chansAssigned := assignChannelsToReader(numChansPerReader, sgChannels)
		for _, chanAssigned := range chansAssigned {
			uniqueChansTotal[chanAssigned] = struct{}{}
			uniqueChans[chanAssigned] = struct{}{}
		}

		// make sure for this call to assignChannelsToReader
		// it didn't return any duplicate channels
		if len(uniqueChans) != numChansPerReader {
			t.Errorf("Expected %d unique chans, got %d.  Chans: %v", numChansPerReader, len(uniqueChans), chansAssigned)
		}

	}

	// Given that we are running so many iterations, we'd expect to see every single channel
	// NOTE: this is theoretically possible to fail sporadically depending on
	// random number generator
	if len(uniqueChansTotal) != len(sgChannels) {
		t.Errorf("Expected %d unique chans, got %d", len(sgChannels), len(uniqueChansTotal))
	}

}
