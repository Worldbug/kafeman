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
		Short: "Create empty config and export to file (default ~/.kafeman/config.yaml)",
		Run: func(cmd *cobra.Command, args []string) {
			// TODO: FIXME: config path
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

			fmt.Fprintf(os.Stdout, "Config created in ~/.config/kafeman/config.yaml\n")
		},
	}

	return cmd
}
