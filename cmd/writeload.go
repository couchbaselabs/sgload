package cmd

import (
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/spf13/cobra"
)

var (
	numWriters               *int
	numChannels              *int
	numDocs                  *int
	docSizeBytes             *int
	maxConcurrentHttpClients *int
)

// writeloadCmd respresents the writeload command
var writeloadCmd = &cobra.Command{
	Use:   "writeload",
	Short: "Generate a write load",
	Long:  `Generate a write load`,
	Run: func(cmd *cobra.Command, args []string) {

		writeLoadSpec := sgload.WriteLoadSpec{
			LoadSpec: sgload.LoadSpec{
				SyncGatewayUrl:       *sgUrl,
				SyncGatewayAdminPort: *sgAdminPort,
				CreateUsers:          *createUsers,
				UserCreds:            *userCreds,
				MockDataStore:        *mockDataStore,
			},
			NumWriters:               *numWriters,
			NumChannels:              *numChannels,
			DocSizeBytes:             *docSizeBytes,
			NumDocs:                  *numDocs,
			MaxConcurrentHttpClients: *maxConcurrentHttpClients,
		}
		if err := writeLoadSpec.Validate(); err != nil {
			log.Fatalf("Invalid parameters: %+v. Error: %v", writeLoadSpec, err)
		}
		writeLoadRunner := sgload.NewWriteLoadRunner(writeLoadSpec)
		if err := writeLoadRunner.Run(); err != nil {
			log.Fatalf("Writeload.Run() failed with: %v", err)
		}

		log.Printf("Finished")

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

	maxConcurrentHttpClients = writeloadCmd.PersistentFlags().Int(
		"maxConcurrentHttpClients",
		20,
		"Caps the number of concurrent outstanding client requests to Sync Gateway.  For example if set to 20, and 20 simulated writers have outstanding requests waiting for responses, the next writer will block until one of those responses is returned",
	)

	// Cobra supports local flags which will only run when this command is called directly
	// writeloadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle" )

}
