package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	numWriters *int
)

// writeloadCmd respresents the writeload command
var writeloadCmd = &cobra.Command{
	Use:   "writeload",
	Short: "Generate a write load",
	Long:  `Generate a write load`,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Work your own magic here
		fmt.Printf("writeload called.  sgUrl: %v\n", *sgUrl)
	},
}

func init() {

	RootCmd.AddCommand(writeloadCmd)

	numWriters = writeloadCmd.PersistentFlags().Int(
		"numwriters",
		100,
		"The number of unique users that will write documents",
	)

	// Cobra supports local flags which will only run when this command is called directly
	// writeloadCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle" )

}
