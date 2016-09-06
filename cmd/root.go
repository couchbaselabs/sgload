package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile     string
	sgUrl       *string
	createUsers *bool
	userCreds   *string
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
		"http://localhost:4984/db",
		"The public Sync Gateway URL including port and database, eg: http://localhost:4984/db",
	)

	createUsers = RootCmd.PersistentFlags().Bool(
		"createusers",
		true,
		"Whether or not to create users.  Set to false if the users already exist",
	)

	userCreds = RootCmd.PersistentFlags().String(
		"usercreds",
		"{\"foo\":\"passw0rd\"}",
		"The usernames/passwords of the users to use for testing if createusers set to false.  Must be equal to numbrer of writers",
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
