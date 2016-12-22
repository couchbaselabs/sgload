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
	skipWriteload         *bool
	readLoadNumWriters    *int
	readLoadCreateWriters *bool
	readLoadFeedType      *string
	logger                log15.Logger
)

// readloadCmd respresents the readload command
var readloadCmd = &cobra.Command{
	Use:   "readload",
	Short: "Generate a read-only load",
	Long: `Generate a read-only load.  This will kick off a write load by default, 
or it can pass a test session id from a previously run write load.`,
	Run: func(cmd *cobra.Command, args []string) {

		// Setup logger
		logger := sgload.Logger()

		loadSpec := createLoadSpecFromArgs()
		sgload.SetLogLevel(loadSpec.LogLevel)

		readLoadSpec := sgload.ReadLoadSpec{
			LoadSpec:                  loadSpec,
			NumReaders:                *numReaders,
			NumChansPerReader:         *numChansPerReader,
			CreateReaders:             *createReaders,
			SkipWriteLoadSetup:        *skipWriteload,
			NumRevGenerationsExpected: 1, // Expect writer to add one rev
			FeedType:                  sgload.ChangesFeedType(*readLoadFeedType),
		}

		logger.Info("Running readload scenario", "readLoadSpec", readLoadSpec)

		if *skipWriteload == false {

			logger.Info("Running writeload scenario")
			if err := runWriteLoadScenarioReadLoad(loadSpec); err != nil {
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

func runWriteLoadScenarioReadLoad(loadSpec sgload.LoadSpec) error {

	writeLoadSpec := sgload.WriteLoadSpec{
		LoadSpec:      loadSpec,
		NumWriters:    *readLoadNumWriters,
		CreateWriters: *readLoadCreateWriters,
	}
	if err := writeLoadSpec.Validate(); err != nil {
		logger.Crit("Invalid loadspec", "error", err, "writeLoadSpec", writeLoadSpec)
	}
	writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec)

	return writeLoadRunner.Run()

}

func init() {

	RootCmd.AddCommand(readloadCmd)

	numReaders = readloadCmd.PersistentFlags().Int(
		NUM_READERS_CMD_NAME,
		NUM_READERS_CMD_DEFAULT,
		NUM_READERS_CMD_DESC,
	)

	numChansPerReader = readloadCmd.PersistentFlags().Int(
		NUM_CHANS_PER_READER_CMD_NAME,
		NUM_CHANS_PER_READER_CMD_DEFAULT,
		NUM_CHANS_PER_READER_CMD_DESC,
	)

	createReaders = readloadCmd.PersistentFlags().Bool(
		CREATE_READERS_CMD_NAME,
		CREATE_READERS_CMD_DEFAULT,
		CREATE_READERS_CMD_DESC,
	)

	skipWriteload = readloadCmd.PersistentFlags().Bool(
		SKIP_WRITELOAD_CMD_NAME,
		SKIP_WRITELOAD_CMD_DEFAULT,
		SKIP_WRITELOAD_CMD_DESC,
	)

	readLoadNumWriters = readloadCmd.PersistentFlags().Int(
		NUM_WRITERS_CMD_NAME,
		NUM_WRITERS_CMD_DEFAULT,
		NUM_WRITERS_CMD_DESC,
	)

	readLoadCreateWriters = readloadCmd.PersistentFlags().Bool(
		CREATE_WRITERS_CMD_NAME,
		CREATE_WRITERS_CMD_DEFAULT,
		CREATE_WRITERS_CMD_DESC,
	)

	readLoadFeedType = readloadCmd.PersistentFlags().String(
		FEED_TYPE_CMD_NAME,
		FEED_TYPE_CMD_DEFAULT,
		FEED_TYPE_CMD_DESC,
	)

}
