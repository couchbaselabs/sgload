package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile        string
	sgUrl          *string
	sgAdminPort    *int
	mockDataStore  *bool
	statsdEndpoint *string
	statsdEnabled  *bool
)

// This represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "sgload",
	Short: "Sync Gateway Load Generator",
	Long:  `Generate a load against Sync Gateway`,

	// Uncomment if bare command is needed
	// Run: func(cmd *cobra.Command, args []string) {
	// 	log.Printf("hello")
	// },
}

//Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	// Here you will define your flags and configuration settings
	// Cobra supports Persistent Flags which if defined here will be global for your application

	sgUrl = RootCmd.PersistentFlags().String(
		"sg-url",
		"http://localhost:4984/db/",
		"The public Sync Gateway URL including port and database and trailing slash, eg: http://localhost:4984/db/",
	)

	sgAdminPort = RootCmd.PersistentFlags().Int(
		"sg-admin-port",
		4985,
		"The Sync Gateway admin port.  NOTE: if SG is not on the same box you will need to setup SSH port forwarding, VPN, or configure SG to allow access",
	)

	mockDataStore = RootCmd.PersistentFlags().Bool(
		"mockdatastore",
		false,
		"Add this flag to use the Mock DataStore rather than hitting a real sync gateway instance",
	)

	statsdEndpoint = RootCmd.PersistentFlags().String(
		"statsdendpoint",
		"localhost:8125",
		"The statds endpoint to push stats to via UDP.  If non-existent, will not break anything.",
	)

	statsdEnabled = RootCmd.PersistentFlags().Bool(
		"statsdenabled",
		false,
		"Add this flag to push stats to statsdendpoint",
	)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.sgload.yaml)")

	// Cobra also supports local flags which will only run when this action is called directly
	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}

// Read in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}

	viper.SetConfigName(".sgload") // name of config file (without extension)
	viper.AddConfigPath("$HOME")   // adding home directory as first search path
	viper.AutomaticEnv()           // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
