package cmd

import (
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/spf13/cobra"
)

var (
	numReaders        *int
	numChansPerReader *int
	createReaders     *bool
	readerCreds       *string
)

// readloadCmd respresents the readload command
var readloadCmd = &cobra.Command{
	Use:   "readload",
	Short: "Generate a read load",
	Long:  `Generate a read load`,
	Run: func(cmd *cobra.Command, args []string) {

		readLoadSpec := sgload.ReadLoadSpec{
			LoadSpec: sgload.LoadSpec{
				SyncGatewayUrl:       *sgUrl,
				SyncGatewayAdminPort: *sgAdminPort,
				MockDataStore:        *mockDataStore,
				StatsdEnabled:        *statsdEnabled,
				StatsdEndpoint:       *statsdEndpoint,
				TestSessionID:        *testSessionID,
				BatchSize:            *batchSize,
			},
			NumReaders:        *numReaders,
			NumChansPerReader: *numChansPerReader,
			CreateReaders:     *createReaders,
			ReaderCreds:       *readerCreds,
		}
		readLoadSpec.GenerateTestSessionID()
		if err := readLoadSpec.Validate(); err != nil {
			log.Fatalf("Invalid parameters: %+v. Error: %v", readLoadSpec, err)
		}

		// TODO: create a writeload spec and runner

		readLoadRunner := sgload.NewReadLoadRunner(readLoadSpec)
		if err := readLoadRunner.Run(); err != nil {
			log.Fatalf("Readload.Run() failed with: %v", err)
		}

	},
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

}
