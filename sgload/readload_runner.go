package sgload

import "github.com/peterbourgon/g2s"

type ReadLoadRunner struct {
	ReadLoadSpec ReadLoadSpec
	StatsdClient *g2s.Statsd
}

func NewReadLoadRunner(rls ReadLoadSpec) *ReadLoadRunner {

	var statsdClient *g2s.Statsd
	var err error

	if rls.StatsdEnabled {
		// statsClient *should* be safe to be shared among multiple
		// goroutines, based on fact that connection returned from Dial
		statsdClient, err = g2s.Dial("udp", rls.StatsdEndpoint)
		if err != nil {
			panic("Couldn't connect to statsd!")
		}
	}

	rls.MustValidate()

	return &ReadLoadRunner{
		ReadLoadSpec: rls,
		StatsdClient: statsdClient,
	}
}

func (rlr ReadLoadRunner) Run() error {

	// Create writer goroutines
	readers, err := rlr.createReaders()
	if err != nil {
		return err
	}
	for _, reader := range readers {
		go reader.Run()
	}

	return nil

}

func (rlr ReadLoadRunner) createReaders() ([]*Reader, error) {
	readers := []*Reader{}
	return readers, nil
}
