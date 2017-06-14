package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile               string
	sgUrl                 *string
	sgAdminPort           *int
	mockDataStore         *bool
	statsdEndpoint        *string
	statsdPrefix          *string
	statsdEnabled         *bool
	testSessionID         *string
	numChannels           *int
	numDocs               *int
	docSizeBytes          *int
	batchSize             *int
	attachSizeBytes       *int
	compressionEnabled    *bool
	expvarProgressEnabled *bool
	logLevelStr           *string
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

	statsdPrefix = RootCmd.PersistentFlags().String(
		"statsdprefix",
		"",
		"The statds prefix to use, for example an api token for a hosted statsd service.",
	)

	statsdEnabled = RootCmd.PersistentFlags().Bool(
		"statsdenabled",
		false,
		"Add this flag to push stats to statsdendpoint",
	)

	testSessionID = RootCmd.PersistentFlags().String(
		"testsessionid",
		"",
		"A unique identifier for this test session, used for generating channel names.  If omitted, a UUID will be auto-generated",
	)

	numChannels = RootCmd.PersistentFlags().Int(
		"numchannels",
		100,
		"The number of unique channels that docs will be distributed to.  Must be less than or equal to total number of docs.  If less than, then multiple docs will be assigned to the same channel.  If equal to, then each doc will get its own channel",
	)

	// NOTE: could also be numDocsPerWriter and total docs would be numWriters * numDocsPerWriter
	numDocs = RootCmd.PersistentFlags().Int(
		"numdocs",
		1000,
		"The number of total docs that will be written.  Will be evenly distributed among writers",
	)

	// NOTE: could also just point to a sample doc or doc templates
	docSizeBytes = RootCmd.PersistentFlags().Int(
		"docsizebytes",
		1024,
		"The size of each doc, in bytes, that will be pushed up to sync gateway",
	)

	batchSize = RootCmd.PersistentFlags().Int(
		"batchsize",
		1,
		"The batch size that will be used for writing docs via bulk_docs endpoint",
	)

	attachSizeBytes = RootCmd.PersistentFlags().Int(
		"attachsizebytes",
		0,
		"The size of the attachment to add in bytes (creates/updates).  Only takes effect if batchsize == 1",
	)

	compressionEnabled = RootCmd.PersistentFlags().Bool(
		"compressionenabled",
		false,
		"Whether gzip compression is enabled",
	)

	expvarProgressEnabled = RootCmd.PersistentFlags().Bool(
		"expvarprogressenabled",
		false,
		"Publish progress stats to expvars",
	)

	logLevelStr = RootCmd.PersistentFlags().String(
		"loglevel",
		"info",
		"Will show all levels up to and including this log level.  Values: critical, error, warn, info, debug",
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
