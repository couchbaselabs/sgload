package cmd

import (
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

var (
	numWriters    *int
	createWriters *bool
	writerCreds   *string
)

// writeloadCmd respresents the writeload command
var writeloadCmd = &cobra.Command{
	Use:   "writeload",
	Short: "Generate a write load",
	Long:  `Generate a write load`,
	Run: func(cmd *cobra.Command, args []string) {

		// Setup logger
		logger := log15.New()

		loadSpec := createLoadSpecFromArgs()
		writeLoadSpec := sgload.WriteLoadSpec{
			LoadSpec:      loadSpec,
			NumWriters:    *numWriters,
			CreateWriters: *createWriters,
			WriterCreds:   *writerCreds,
		}
		if err := writeLoadSpec.Validate(); err != nil {
			log.Fatalf("Invalid parameters: %+v. Error: %v", writeLoadSpec, err)
		}
		writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec, logger)
		if err := writeLoadRunner.Run(); err != nil {
			log.Fatalf("Writeload.Run() failed with: %v", err)
		}

	},
}

func init() {

	RootCmd.AddCommand(writeloadCmd)

	numWriters = writeloadCmd.PersistentFlags().Int(
		"numwriters",
		100,
		"The number of unique users that will write documents.  Each writer runs concurrently in it's own goroutine",
	)

	createWriters = writeloadCmd.PersistentFlags().Bool(
		"createwriters",
		false,
		"Add this flag if you need the test to create SG users for writers.  Otherwise you'll need to specify writercreds",
	)

	writerCreds = writeloadCmd.PersistentFlags().String(
		"writercreds",
		"",
		"The usernames/passwords of the SG users to use for writers in a JSON array form, eg: [{\"foo\":\"passw0rd\"}].  Must be equal to number of writers.  Leave this flag off if using the createwriters flag to create writers",
	)

}
