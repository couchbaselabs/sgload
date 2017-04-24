package cmd

import (
	"fmt"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/spf13/cobra"
	"time"
)

var (
	numWriters    *int
	createWriters *bool
	writerDelayMs *int
)

// writeloadCmd respresents the writeload command
var writeloadCmd = &cobra.Command{
	Use:   "writeload",
	Short: "Generate a write load",
	Long:  `Generate a write load`,
	Run: func(cmd *cobra.Command, args []string) {

		// Setup logger
		logger = sgload.Logger()

		loadSpec := createLoadSpecFromArgs()
		sgload.SetLogLevel(loadSpec.LogLevel)

		delayBetweenWrites := time.Millisecond * time.Duration(*writerDelayMs)

		writeLoadSpec := sgload.WriteLoadSpec{
			LoadSpec:      loadSpec,
			NumWriters:    *numWriters,
			CreateWriters: *createWriters,
			DelayBetweenWrites: delayBetweenWrites,
		}
		if err := writeLoadSpec.Validate(); err != nil {

			panic(fmt.Sprintf("Invalid parameters: %+v. Error: %v", writeLoadSpec, err))
		}
		writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec)
		if err := writeLoadRunner.Run(); err != nil {
			panic(fmt.Sprintf("Writeload.Run() failed with: %v", err))
		}

	},
}

func init() {

	RootCmd.AddCommand(writeloadCmd)

	numWriters = writeloadCmd.PersistentFlags().Int(
		NUM_WRITERS_CMD_NAME,
		NUM_WRITERS_CMD_DEFAULT,
		NUM_WRITERS_CMD_DESC,
	)

	createWriters = writeloadCmd.PersistentFlags().Bool(
		CREATE_WRITERS_CMD_NAME,
		CREATE_WRITERS_CMD_DEFAULT,
		CREATE_WRITERS_CMD_DESC,
	)

	writerDelayMs = writeloadCmd.PersistentFlags().Int(
		WRITER_DELAY_CMD_NAME,
		WRITER_DELAY_CMD_DEFAULT,
		WRITER_DELAY_CMD_DESC,
	)

}
