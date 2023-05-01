package config_cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/logger"
)

func NewConfigSetCluster(configPath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "set-cluster",
		Example:           "kafeman config set-cluster",
		Short:             "Interactively select a cluster",
		Long:              "kafeman config set-cluster [cluster name] or kafeman config set-cluster for interactively select a cluster",
		ValidArgsFunction: completion_cmd.NewClusterCompletion(),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 {
				ok := run_configuration.SetCurrentCluster(args[0])
				if !ok {
					logger.Fatalf("Cluster %s not exist", args[0])
				}

				err := run_configuration.WriteConfiguration()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Can`t save config: %+v", err)
					os.Exit(0)
				}

				return
			}

			var clusterNames []string
			var pos = 0
			for k, cluster := range run_configuration.Config.Clusters {
				clusterNames = append(clusterNames, cluster.Name)
				if cluster.Name == run_configuration.GetCurrentCluster().Name {
					pos = k
				}
			}

			searcher := func(input string, index int) bool {
				cluster := clusterNames[index]
				name := strings.Replace(strings.ToLower(cluster), " ", "", -1)
				input = strings.Replace(strings.ToLower(input), " ", "", -1)
				return strings.Contains(name, input)
			}

			p := promptui.Select{
				Label:     "Select cluster",
				Items:     clusterNames,
				Searcher:  searcher,
				Size:      10,
				CursorPos: pos,
			}

			_, selected, err := p.Run()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can`t select config: %+v", err)
				os.Exit(0)
			}

			ok := run_configuration.SetCurrentCluster(selected)
			if !ok {
				logger.Fatalf("Cluster %s not exist", selected)
			}

			err = run_configuration.WriteConfiguration()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can`t save config: %+v", err)
				os.Exit(0)
			}
		},
	}

	return cmd
}
