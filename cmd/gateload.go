package cmd

import (
	"fmt"
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

var (
	glNumReaders        *int
	glNumWriters        *int
	glWriterCreds       *string
	glNumChansPerReader *int
	glCreateReaders     *bool
	glCreateWriters     *bool
)

var gateloadCmd = &cobra.Command{
	Use:   "gateload",
	Short: "Run the gateload scenario using sgload",
	Long:  `Run the gateload scenario using sgload`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("glNumReaders: %d", *glNumReaders)

		// Setup logger
		logger = log15.New()
		sgload.SetLogger(logger)

		loadSpec := createLoadSpecFromArgs()

		writeLoadSpec := sgload.WriteLoadSpec{
			LoadSpec:      loadSpec,
			NumWriters:    *glNumWriters,
			CreateWriters: *glCreateWriters,
		}

		readLoadSpec := sgload.ReadLoadSpec{
			LoadSpec:          loadSpec,
			NumReaders:        *glNumReaders,
			NumChansPerReader: *glNumChansPerReader,
			CreateReaders:     *glCreateReaders,
		}

		gateLoadSpec := sgload.GateLoadSpec{
			LoadSpec:      loadSpec,
			WriteLoadSpec: writeLoadSpec,
			ReadLoadSpec:  readLoadSpec,
		}
		if err := gateLoadSpec.Validate(); err != nil {

			panic(fmt.Sprintf("Invalid parameters: %+v. Error: %v", gateLoadSpec, err))
		}
		gateLoadRunner := sgload.NewGateLoadRunner(gateLoadSpec)
		if err := gateLoadRunner.Run(); err != nil {
			panic(fmt.Sprintf("Gateload.Run() failed with: %v", err))
		}

	},
}

func init() {

	RootCmd.AddCommand(gateloadCmd)

	glNumReaders = gateloadCmd.PersistentFlags().Int(
		NUM_READERS_CMD_NAME,
		NUM_READERS_CMD_DEFAULT,
		NUM_READERS_CMD_DESC,
	)

	glNumWriters = gateloadCmd.PersistentFlags().Int(
		NUM_WRITERS_CMD_NAME,
		NUM_WRITERS_CMD_DEFAULT,
		NUM_WRITERS_CMD_DESC,
	)

	glCreateWriters = gateloadCmd.PersistentFlags().Bool(
		CREATE_WRITERS_CMD_NAME,
		CREATE_WRITERS_CMD_DEFAULT,
		CREATE_WRITERS_CMD_DESC,
	)

	glNumChansPerReader = gateloadCmd.PersistentFlags().Int(
		NUM_CHANS_PER_READER_CMD_NAME,
		NUM_CHANS_PER_READER_CMD_DEFAULT,
		NUM_CHANS_PER_READER_CMD_DESC,
	)

	glCreateReaders = gateloadCmd.PersistentFlags().Bool(
		CREATE_READERS_CMD_NAME,
		CREATE_READERS_CMD_DEFAULT,
		CREATE_READERS_CMD_DESC,
	)

}
