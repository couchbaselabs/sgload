package sgload

import "log"

type ReadLoadSpec struct {
	LoadSpec
	CreateReaders             bool // Whether or not to create users for readers
	NumReaders                int
	NumChansPerReader         int
	NumRevGenerationsExpected int
	SkipWriteLoadSetup        bool            // By default the readload scenario runs the writeload scenario first.  If this is true, it will skip the writeload scenario.
	FeedType                  ChangesFeedType // "Normal" or "Longpoll"

}

func (rls ReadLoadSpec) Validate() error {

	if err := rls.LoadSpec.Validate(); err != nil {
		return err
	}

	return nil
}

// Validate this spec or panic
func (rls ReadLoadSpec) MustValidate() {
	if err := rls.Validate(); err != nil {
		log.Panicf("Invalid ReadLoadSpec: %+v. Error: %v", rls, err)
	}
}
