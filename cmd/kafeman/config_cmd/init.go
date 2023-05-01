package config_cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/config"
)

func NewConfigInitCMD(configPath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: fmt.Sprintf("Create empty config and export to file (default %s)\n", run_configuration.ConfigPath),
		Run: func(cmd *cobra.Command, args []string) {
			run_configuration.Config = &config.Configuration{
				CurrentCluster: "local",
				Clusters: []config.Cluster{
					{
						Name:    "local",
						Brokers: []string{"localhost:9092"},
					},
				},
			}

			err := run_configuration.WriteConfiguration()
			if err != nil {
				common.ExitWithErr("Can`t save config: %+v", err)
			}

			fmt.Fprintf(os.Stdout, "Config created in %s\n", run_configuration.ConfigPath)
		},
	}

	return cmd
}
