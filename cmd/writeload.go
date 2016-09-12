package cmd

import (
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/peterbourgon/g2s"
	"github.com/spf13/cobra"
)

var (
	numWriters   *int
	numChannels  *int
	numDocs      *int
	docSizeBytes *int
	batchSize    *int
)

// writeloadCmd respresents the writeload command
var writeloadCmd = &cobra.Command{
	Use:   "writeload",
	Short: "Generate a write load",
	Long:  `Generate a write load`,
	Run: func(cmd *cobra.Command, args []string) {

		statdsClient, err := g2s.Dial("udp", "localhost:8125")
		if err != nil {
			// do something
			log.Printf("Error connecting to statsd: %v", err)
		}

		statdsClient.Counter(1.0, "test_key", 1)

		writeLoadSpec := sgload.WriteLoadSpec{
			LoadSpec: sgload.LoadSpec{
				SyncGatewayUrl:       *sgUrl,
				SyncGatewayAdminPort: *sgAdminPort,
				CreateUsers:          *createUsers,
				UserCreds:            *userCreds,
				MockDataStore:        *mockDataStore,
			},
			NumWriters:   *numWriters,
			NumChannels:  *numChannels,
			DocSizeBytes: *docSizeBytes,
			NumDocs:      *numDocs,
			BatchSize:    *batchSize,
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

	// Cobra supports local flags which will only run when this command is called directly
	// writeloadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle" )

}
