package cmd

import (
	"os"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/inconshreveable/log15"
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
	logger                log15.Logger
)

// readloadCmd respresents the readload command
var readloadCmd = &cobra.Command{
	Use:   "readload",
	Short: "Generate a read load",
	Long:  `Generate a read load`,
	Run: func(cmd *cobra.Command, args []string) {

		// Setup logger
		logger = log15.New()
		sgload.SetLogger(logger)

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

			logger.Info("Running writeload scenario")
			if err := runWriteLoadScenario(loadSpec); err != nil {
				logger.Crit("Failed to run writeload", "error", err)
				os.Exit(1)
			}
			logger.Info("Finished running writeload scenario")

		}

		if err := readLoadSpec.Validate(); err != nil {
			logger.Crit("Invalid loadspec", "error", err, "readLoadSpec", readLoadSpec)
			os.Exit(1)

		}

		logger.Info("Running readload scenario")
		readLoadRunner := sgload.NewReadLoadRunner(readLoadSpec)
		if err := readLoadRunner.Run(); err != nil {
			logger.Crit("Readload.Run() failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Finished running readload scenario")

	},
}

func runWriteLoadScenario(loadSpec sgload.LoadSpec) error {

	writeLoadSpec := sgload.WriteLoadSpec{
		LoadSpec:      loadSpec,
		NumWriters:    *readLoadNumWriters,
		CreateWriters: *readLoadCreateWriters,
		WriterCreds:   *readLoadWriterCreds,
	}
	if err := writeLoadSpec.Validate(); err != nil {
		logger.Crit("Invalid loadspec", "error", err, "writeLoadSpec", writeLoadSpec)
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
		true,
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
		true,
		"Add this flag if you need the test to create SG users for writers.  Otherwise you'll need to specify writercreds",
	)

	readLoadWriterCreds = readloadCmd.PersistentFlags().String(
		"writercreds",
		"",
		"The usernames/passwords of the SG users to use for writers in a JSON array form, eg: [{\"foo\":\"passw0rd\"}].  Must be equal to number of writers.  Leave this flag off if using the createwriters flag to create writers",
	)

}
