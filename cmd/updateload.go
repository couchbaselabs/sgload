package cmd

import (
	"fmt"
	"os"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

var (
	numRevsPerDoc           *int
	numUpdaters             *int
	skipWriteload           *bool
	updateLoadNumWriters    *int
	updateLoadCreateWriters *bool
)

// updateloadCmd respresents the updateload command
var updateloadCmd = &cobra.Command{
	Use:   "updateload",
	Short: "Update existing docs",
	Long:  `Update existing docs`,
	Run: func(cmd *cobra.Command, args []string) {

		// Setup logger
		logger = log15.New()
		sgload.SetLogger(logger)

		loadSpec := createLoadSpecFromArgs()
		updateLoadSpec := sgload.UpdateLoadSpec{
			LoadSpec:      loadSpec,
			NumRevsPerDoc: *numRevsPerDoc,
			NumUpdaters:   *numUpdaters,
		}

		if *skipWriteload == false {

			logger.Info("Running writeload scenario")
			if err := runWriteLoadScenario(loadSpec); err != nil {
				logger.Crit("Failed to run writeload", "error", err)
				os.Exit(1)
			}
			logger.Info("Finished running writeload scenario")

		}

		if err := updateLoadSpec.Validate(); err != nil {

			panic(fmt.Sprintf("Invalid parameters: %+v. Error: %v", updateLoadSpec, err))
		}
		updateLoadRunner := sgload.NewUpdateLoadRunner(updateLoadSpec)
		if err := updateLoadRunner.Run(); err != nil {
			panic(fmt.Sprintf("Writeload.Run() failed with: %v", err))
		}

	},
}

func init() {

	RootCmd.AddCommand(updateloadCmd)

	skipWriteload = updateloadCmd.PersistentFlags().Bool(
		SKIP_WRITELOAD_CMD_NAME,
		SKIP_WRITELOAD_CMD_DEFAULT,
		SKIP_WRITELOAD_CMD_DESC,
	)

	numUpdaters = updateloadCmd.PersistentFlags().Int(
		NUM_UPDATERS_CMD_NAME,
		NUM_UPDATERS_CMD_DEFAULT,
		NUM_UPDATERS_CMD_DESC,
	)

	updateLoadNumWriters = updateloadCmd.PersistentFlags().Int(
		NUM_WRITERS_CMD_NAME,
		NUM_WRITERS_CMD_DEFAULT,
		NUM_WRITERS_CMD_DESC,
	)

	updateLoadCreateWriters = updateloadCmd.PersistentFlags().Bool(
		CREATE_WRITERS_CMD_NAME,
		CREATE_WRITERS_CMD_DEFAULT,
		CREATE_WRITERS_CMD_DESC,
	)

}
