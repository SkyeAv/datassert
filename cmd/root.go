package cmd

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "datassert",
	Short: "golang",
	Long:  "golang",
}

func Execute() {
	rootCmd.Execute()
}
