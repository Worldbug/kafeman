package command

import (
	"kafeman/internal/config"

	"github.com/spf13/cobra"
)

var (
	configPath = ""
)

func init() {
	RootCMD.AddCommand(ConfigCMD)
	RootCMD.PersistentFlags().StringVar(&configPath, "config", "", "config file (default is $HOME/.kafeman/config.yml)")

	ConfigCMD.AddCommand(ExportConfig)

}

var ConfigCMD = &cobra.Command{
	Use:   "config",
	Short: "Handle kafman configuration",
}

var ExportConfig = &cobra.Command{
	Use:   "export",
	Short: "Create empty config and export to file (default ~/.kafeman/config.yml)",
	Run: func(cmd *cobra.Command, args []string) {
		config.ExportConfig(configPath)
	},
}
