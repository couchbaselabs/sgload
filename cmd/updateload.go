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
	updateLoadSkipWriteload *bool
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

		if *updateLoadSkipWriteload == false {

			logger.Info("Running writeload scenario")
			if err := runWriteLoadScenarioUpdateLoad(loadSpec); err != nil {
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

func runWriteLoadScenarioUpdateLoad(loadSpec sgload.LoadSpec) error {

	writeLoadSpec := sgload.WriteLoadSpec{
		LoadSpec:      loadSpec,
		NumWriters:    *updateLoadNumWriters,
		CreateWriters: *updateLoadCreateWriters,
	}
	if err := writeLoadSpec.Validate(); err != nil {
		logger.Crit("Invalid loadspec", "error", err, "writeLoadSpec", writeLoadSpec)
	}
	writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec)

	return writeLoadRunner.Run()

}

func init() {

	RootCmd.AddCommand(updateloadCmd)

	updateLoadSkipWriteload = updateloadCmd.PersistentFlags().Bool(
		SKIP_WRITELOAD_CMD_NAME,
		SKIP_WRITELOAD_CMD_DEFAULT,
		SKIP_WRITELOAD_CMD_DESC,
	)

	numRevsPerDoc = updateloadCmd.PersistentFlags().Int(
		NUM_REVS_PER_DOC_CMD_NAME,
		NUM_REVS_PER_DOC_CMD_DEFAULT,
		NUM_REVS_PER_DOC_CMD_DESC,
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
