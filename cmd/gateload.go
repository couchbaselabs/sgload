package cmd

import (
	"fmt"
	"log"

	"github.com/couchbaselabs/sgload/sgload"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

var (
	glNumReaders *int
	glNumWriters *int
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
		gateLoadSpec := sgload.GateLoadSpec{
			LoadSpec:   loadSpec,
			NumWriters: *glNumWriters,
			NumReaders: *glNumReaders,
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

}
