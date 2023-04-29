package config_cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/worldbug/kafeman/internal/command"
	"github.com/worldbug/kafeman/internal/config"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

func newConfigOptions() *configOptions {
	return &configOptions{
		// configPath:      "",
		// clusterOverride: "",
		// failTolerance:   false,
		// quiet:           false,
	}
}

// TODO:
type configOptions struct {
	// configPath      string
	// clusterOverride string
	// failTolerance   bool
	// quiet           bool
}

// TODO: refactor
func NewConfigCMD(kafemanCMD *cobra.Command) *cobra.Command {
	// отвязать настройки конфига от это го места

	//	options := newConfigOptions()

	// if config not inited
	if len(config.Config.Clusters) == 0 {
		config.Config.CurrentCluster = "local"
		config.Config.Clusters = append(config.Config.Clusters, config.Cluster{
			Name:    "local",
			Brokers: []string{"localhost:9092"},
		})
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
	cmd.AddCommand(NewConfigSelectCluster(configPath))
	// TODO: bug NewConfigCurrentContextCMD
	cmd.AddCommand(NewConfigCurrentContextCMD(configPath))
	// ConfigCMD.AddCommand(configAddEventhub)

	cmd.AddCommand(NewConfigInitCMD(configPath))

	// TODO: may not work
	fmt.Println(
		"orig: ", config.Config.CurrentCluster,
		// "\nnew:", options.clusterOverride,
	)

	return cmd
}

func NewConfigCurrentContextCMD(configPath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "current-context",
		Short: "Displays the current context",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(os.Stdout, "%s\n", config.Config.GetCurrentCluster().Name)
		},
	}

	return cmd
}

func NewConfigSelectCluster(configPath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "select-cluster",
		Aliases: []string{"ls"},
		Example: "kafeman config select-cluster",
		Short:   "Interactively select a cluster",
		// ValidArgsFunction: completion_cmd.NewClusterCompletion(config),
		Run: func(cmd *cobra.Command, args []string) {
			var clusterNames []string
			var pos = 0
			for k, cluster := range config.Config.Clusters {
				clusterNames = append(clusterNames, cluster.Name)
				if cluster.Name == config.Config.GetCurrentCluster().Name {
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

			config.Config.SetCurrentCluster(selected)
			err = config.SaveConfig(config.Config, configPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can`t save config: %+v", err)
				os.Exit(0)
			}
		},
	}

	return cmd
}

func NewConfigInitCMD(configPath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create empty config and export to file (default ~/.kafeman/config.yml)",
		Run: func(cmd *cobra.Command, args []string) {
			err := config.ExportConfig(configPath)
			if err != nil {
				command.ExitWithErr("Can`t save config: %+v", err)
			}

			fmt.Fprintf(os.Stdout, "Config created in ~/.config/kafeman/config.yml\n")
		},
	}

	return cmd
}
