package sgload

import "log"

type UpdateLoadSpec struct {
	LoadSpec
	NumUpdatesPerDoc int // The total number of revisions to add per doc
	NumRevsPerUpdate int // The number of revisions to add per update
	NumUpdaters      int // The number of updater goroutines
}

func (uls UpdateLoadSpec) Validate() error {
	if err := uls.LoadSpec.Validate(); err != nil {
		return err
	}
	return nil
}

// Validate this spec or panic
func (uls UpdateLoadSpec) MustValidate() {
	if err := uls.Validate(); err != nil {
		log.Panicf("Invalid UpdateLoadSpec: %+v. Error: %v", uls, err)
	}
}
