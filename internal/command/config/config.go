package config_cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	configProvider "github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/logger"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

func newConfigOptions() configOptions {
	return configOptions{
		configPath:      "",
		clusterOverride: "",
		failTolerance:   false,
		quiet:           false,
	}
}

type configOptions struct {
	configPath      string
	clusterOverride string
	failTolerance   bool
	quiet           bool
}

// TODO: pointer
func NewConfigCMD(kafeman *cobra.Command, config configProvider.Config) *cobra.Command {
	options := newConfigOptions()

	// if config not inited
	if len(config.Clusters) == 0 {
		config.CurrentCluster = "local"
		config.Clusters = append(config.Clusters, configProvider.Cluster{
			Name:    "local",
			Brokers: []string{"localhost:9092"},
		})
	}

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Handle kafman configuration",
	}

	kafeman.AddCommand(cmd)
	kafeman.PersistentFlags().StringVar(&options.configPath, "config", "", "config file (default is $HOME/.kafeman/config.yml)")
	kafeman.PersistentFlags().StringVarP(&options.clusterOverride, "cluster", "c", "", "set a temporary current cluster")
	kafeman.PersistentFlags().BoolVar(&options.failTolerance, "tolerance", false, "don't crash on errors")
	kafeman.PersistentFlags().BoolVar(&options.quiet, "quiet", false, "do not print info and errors")
	kafeman.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion(config))

	// TODO: may not work
	if options.clusterOverride != "" {
		config.CurrentCluster = options.clusterOverride
	}

	logger.InitLogger(options.failTolerance, options.quiet)

	return cmd
}

func NewConfigCurrentContextCMD(config configProvider.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current-context",
		Short: "Displays the current context",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(os.Stdout, "%s\n", config.GetCurrentCluster().Name)
		},
	}

	// TODO:
	// ConfigCMD.AddCommand(configImportCmd)
	// ConfigCMD.AddCommand(configUseCmd)
	// ConfigCMD.AddCommand(configLsCmd)
	// ConfigCMD.AddCommand(configAddClusterCmd)
	// ConfigCMD.AddCommand(configRemoveClusterCmd)
	cmd.AddCommand(NewConfigSelectCluster(config))
	cmd.AddCommand(NewConfigCurrentContextCMD(config))
	// ConfigCMD.AddCommand(configAddEventhub)

	cmd.AddCommand(NewConfigInitCMD())

	return cmd
}

func NewConfigSelectCluster(config configProvider.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "select-cluster",
		Aliases: []string{"ls"},
		Example: "kafeman config select-cluster",
		Short:   "Interactively select a cluster",
		Run: func(cmd *cobra.Command, args []string) {
			var clusterNames []string
			var pos = 0
			for k, cluster := range config.Clusters {
				clusterNames = append(clusterNames, cluster.Name)
				if cluster.Name == config.GetCurrentCluster().Name {
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

			config.SetCurrentCluster(selected)
			err = configProvider.SaveConfig(config, configPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can`t save config: %+v", err)
				os.Exit(0)
			}
		},
	}

	return cmd
}

func NewConfigInitCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create empty config and export to file (default ~/.kafeman/config.yml)",
		Run: func(cmd *cobra.Command, args []string) {
			err := configProvider.ExportConfig(configPath)
			if err != nil {
				command.ExitWithErr("Can`t save config: %+v", err)
			}

			fmt.Fprintf(os.Stdout, "Config created in ~/.config/kafeman/config.yml\n")
		},
	}

	return cmd
}
