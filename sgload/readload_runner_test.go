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

	uniqueChans := map[string]interface{}{}

	for i := 0; i < 100; i++ {
		chansAssigned := assignChannelsToReader(numChansPerReader, sgChannels)
		for _, chanAssigned := range chansAssigned {
			uniqueChans[chanAssigned] = struct{}{}
		}
	}

	if len(uniqueChans) != len(sgChannels) {
		t.Errorf("Expected %d unique chans, got %d", len(sgChannels), len(uniqueChans))
	}

}
