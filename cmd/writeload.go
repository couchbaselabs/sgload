package cmd

import (
	"fmt"
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/satori/go.uuid"
	"github.com/spf13/cobra"
)

var (
	numWriters    *int
	numChannels   *int
	numDocs       *int
	docSizeBytes  *int
	batchSize     *int
	createWriters *bool
	writerCreds   *string
)

// writeloadCmd respresents the writeload command
var writeloadCmd = &cobra.Command{
	Use:   "writeload",
	Short: "Generate a write load",
	Long:  `Generate a write load`,
	Run: func(cmd *cobra.Command, args []string) {

		// If the user didn't pass in a test session id, autogen one
		if *testSessionID == "" {
			*testSessionID = NewUuid()
		}

		writeLoadSpec := sgload.WriteLoadSpec{
			LoadSpec: sgload.LoadSpec{
				SyncGatewayUrl:       *sgUrl,
				SyncGatewayAdminPort: *sgAdminPort,
				MockDataStore:        *mockDataStore,
				StatsdEnabled:        *statsdEnabled,
				StatsdEndpoint:       *statsdEndpoint,
				TestSessionID:        *testSessionID,
			},
			NumWriters:    *numWriters,
			NumChannels:   *numChannels,
			DocSizeBytes:  *docSizeBytes,
			NumDocs:       *numDocs,
			BatchSize:     *batchSize,
			CreateWriters: *createWriters,
			WriterCreds:   *writerCreds,
		}
		if err := writeLoadSpec.Validate(); err != nil {
			log.Fatalf("Invalid parameters: %+v. Error: %v", writeLoadSpec, err)
		}
		writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec)
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
		"The number of unique users that will write documents",
	)

	numChannels = writeloadCmd.PersistentFlags().Int(
		"numchannels",
		100,
		"The number of unique channels that docs will be distributed to.  Must be less than or equal to total number of docs.  If less than, then multiple docs will be assigned to the same channel.  If equal to, then each doc will get its own channel",
	)

	// NOTE: could also be numDocsPerWriter and total docs would be numWriters * numDocsPerWriter
	numDocs = writeloadCmd.PersistentFlags().Int(
		"numdocs",
		1000,
		"The number of total docs that will be written.  Will be evenly distributed among writers",
	)

	// NOTE: could also just point to a sample doc or doc templates
	docSizeBytes = writeloadCmd.PersistentFlags().Int(
		"docsizebytes",
		1024,
		"The size of each doc, in bytes, that will be pushed up to sync gateway",
	)

	batchSize = writeloadCmd.PersistentFlags().Int(
		"batchsize",
		1,
		"The batch size that will be used for writing docs via bulk_docs endpoint",
	)

	createWriters = RootCmd.PersistentFlags().Bool(
		"createwriters",
		false,
		"Add this flag if you need the test to create SG users for writers.  Otherwise you'll need to specify writercreds",
	)

	writerCreds = RootCmd.PersistentFlags().String(
		"writercreds",
		"",
		"The usernames/passwords of the SG users to use for writers in a JSON array form, eg: [{\"foo\":\"passw0rd\"}].  Must be equal to number of writers.  Leave this flag off if using the createwriters flag to create writers",
	)

}

func NewUuid() string {
	u4 := uuid.NewV4()
	return fmt.Sprintf("%s", u4)
}
