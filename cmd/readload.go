package cmd

import (
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/spf13/cobra"
)

var (
	numReaders            *int
	numChansPerReader     *int
	createReaders         *bool
	readerCreds           *string
	skipWriteload         *bool
	readLoadNumWriters    *int    // Hack alert: duplicate this CLI writeload arg
	readLoadCreateWriters *bool   // Hack alert: duplicate this CLI writeload arg
	readLoadWriterCreds   *string // Hack alert: duplicate this CLI writeload arg
)

// readloadCmd respresents the readload command
var readloadCmd = &cobra.Command{
	Use:   "readload",
	Short: "Generate a read load",
	Long:  `Generate a read load`,
	Run: func(cmd *cobra.Command, args []string) {

		loadSpec := createLoadSpecFromArgs()

		readLoadSpec := sgload.ReadLoadSpec{
			LoadSpec:           loadSpec,
			NumReaders:         *numReaders,
			NumChansPerReader:  *numChansPerReader,
			CreateReaders:      *createReaders,
			ReaderCreds:        *readerCreds,
			SkipWriteLoadSetup: *skipWriteload,
		}

		if *skipWriteload == false {

			log.Printf("Running writeload scenario")
			if err := runWriteLoadScenario(loadSpec); err != nil {
				log.Fatalf("Failed to run writeload: %v", err)
			}
			log.Printf("Finished running writeload scenario")

		}

		if err := readLoadSpec.Validate(); err != nil {
			log.Fatalf("Invalid readloadSpec parameters: %+v. Error: %v", readLoadSpec, err)
		}

		log.Printf("Running readload scenario")
		readLoadRunner := sgload.NewReadLoadRunner(readLoadSpec)
		if err := readLoadRunner.Run(); err != nil {
			log.Fatalf("Readload.Run() failed with: %v", err)
		}
		log.Printf("Finished running readload scenario")

	},
}

func runWriteLoadScenario(loadSpec sgload.LoadSpec) error {

	log.Printf("createWriters: %v. ", *createWriters)

	writeLoadSpec := sgload.WriteLoadSpec{
		LoadSpec:      loadSpec,
		NumWriters:    *readLoadNumWriters,
		CreateWriters: *readLoadCreateWriters,
		WriterCreds:   *readLoadWriterCreds,
	}
	if err := writeLoadSpec.Validate(); err != nil {
		log.Fatalf("Invalid writeLoadSpec parameters: %+v. Error: %v", writeLoadSpec, err)
	}
	writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec)

	return writeLoadRunner.Run()

}

func createLoadSpecFromArgs() sgload.LoadSpec {

	loadSpec := sgload.LoadSpec{
		SyncGatewayUrl:       *sgUrl,
		SyncGatewayAdminPort: *sgAdminPort,
		MockDataStore:        *mockDataStore,
		StatsdEnabled:        *statsdEnabled,
		StatsdEndpoint:       *statsdEndpoint,
		TestSessionID:        *testSessionID,
		BatchSize:            *batchSize,
		NumChannels:          *numChannels,
		DocSizeBytes:         *docSizeBytes,
		NumDocs:              *numDocs,
	}
	loadSpec.GenerateTestSessionID()
	return loadSpec

}

func init() {

	RootCmd.AddCommand(readloadCmd)

	numReaders = readloadCmd.PersistentFlags().Int(
		"numreaders",
		100,
		"The number of unique readers that will read documents.  Each reader runs concurrently in it's own goroutine",
	)

	numChansPerReader = readloadCmd.PersistentFlags().Int(
		"num-chans-per-reader",
		1,
		"The number of channels that each reader has access to.",
	)

	createReaders = readloadCmd.PersistentFlags().Bool(
		"createreaders",
		false,
		"Add this flag if you need the test to create SG users for readers.  Otherwise you'll need to specify readercreds",
	)

	readerCreds = readloadCmd.PersistentFlags().String(
		"readercreds",
		"",
		"The usernames/passwords of the SG users to use for readers in a JSON array form, eg: [{\"foo\":\"passw0rd\"}].  Must be equal to number of readers.  Leave this flag off if using the createwriters flag to create readers",
	)

	skipWriteload = readloadCmd.PersistentFlags().Bool(
		"skipwriteload",
		false,
		"By default the readload will first run the corresponding writeload, so that it has documents to read, but set this flag if you've run that step separately",
	)

	readLoadNumWriters = readloadCmd.PersistentFlags().Int(
		"numwriters",
		100,
		"The number of unique users that will write documents.  Each writer runs concurrently in it's own goroutine",
	)

	readLoadCreateWriters = readloadCmd.PersistentFlags().Bool(
		"createwriters",
		false,
		"Add this flag if you need the test to create SG users for writers.  Otherwise you'll need to specify writercreds",
	)

	readLoadWriterCreds = readloadCmd.PersistentFlags().String(
		"writercreds",
		"",
		"The usernames/passwords of the SG users to use for writers in a JSON array form, eg: [{\"foo\":\"passw0rd\"}].  Must be equal to number of writers.  Leave this flag off if using the createwriters flag to create writers",
	)

}
