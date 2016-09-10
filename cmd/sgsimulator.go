package cmd

import (
	"github.com/couchbaselabs/sgload/sgsimulator"
	"github.com/spf13/cobra"
)

var (
	db *string
)

// sgsimulatorCmd respresents the sgsimulator command
var sgsimulatorCmd = &cobra.Command{
	Use:   "sgsimulator",
	Short: "Run a Sync Gateway simulator",
	Long:  `Run a Sync Gateway simulator that responds to requests but does not save any data`,
	Run: func(cmd *cobra.Command, args []string) {
		sgSimulator := sgsimulator.NewSGSimulator(*db)
		sgSimulator.Run()
	},
}

func init() {
	RootCmd.AddCommand(sgsimulatorCmd)

	// Here you will define your flags and configuration settings

	// Cobra supports Persistent Flags which will work for this command and all subcommands
	db = sgsimulatorCmd.PersistentFlags().String("db", "db", "The database to respond to")

	// Cobra supports local flags which will only run when this command is called directly
	// sgsimulatorCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle" )

}
