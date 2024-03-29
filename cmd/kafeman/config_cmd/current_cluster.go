package config_cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
)

func NewConfigCurrentClusterCMD(configPath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current-cluster",
		Short: "Displays the current cluster",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(os.Stdout, "%s\n", run_configuration.GetCurrentCluster().Name)
		},
	}

	return cmd
}
