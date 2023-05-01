package config_cmd

import (
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/config"

	"github.com/spf13/cobra"
)

// TODO: refactor
func NewConfigCMD() *cobra.Command {
	// if config not inited
	if len(run_configuration.Config.Clusters) == 0 {
		run_configuration.SetCurrentCluster("local")
		run_configuration.SetCluster(
			config.Cluster{
				Name:    "local",
				Brokers: []string{"localhost:9092"},
			},
		)
	}

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Handle kafman configuration",
	}

	// TODO:
	configPath := ""
	// ConfigCMD.AddCommand(configImportCmd)
	// ConfigCMD.AddCommand(configUseCmd)
	// ConfigCMD.AddCommand(configLsCmd)
	// ConfigCMD.AddCommand(configAddClusterCmd)
	// ConfigCMD.AddCommand(configRemoveClusterCmd)
	cmd.AddCommand(NewConfigSetCluster(configPath))
	cmd.AddCommand(NewConfigCurrentClusterCMD(configPath))
	// ConfigCMD.AddCommand(configAddEventhub)

	cmd.AddCommand(NewConfigInitCMD(configPath))

	return cmd
}
