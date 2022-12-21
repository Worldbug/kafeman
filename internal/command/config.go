package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/worldbug/kafeman/internal/config"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var (
	configPath      = ""
	clusterOverride = ""
)

func init() {
	RootCMD.AddCommand(ConfigCMD)
	RootCMD.PersistentFlags().StringVar(&configPath, "config", "", "config file (default is $HOME/.kafeman/config.yml)")
	RootCMD.PersistentFlags().StringVarP(&clusterOverride, "cluster", "c", "", "set a temporary current cluster")
	// ConfigCMD.AddCommand(configImportCmd)
	// ConfigCMD.AddCommand(configUseCmd)
	// ConfigCMD.AddCommand(configLsCmd)
	// ConfigCMD.AddCommand(configAddClusterCmd)
	// ConfigCMD.AddCommand(configRemoveClusterCmd)
	ConfigCMD.AddCommand(ConfigSelectCluster)
	ConfigCMD.AddCommand(ConfigCurrentContextCMD)
	// ConfigCMD.AddCommand(configAddEventhub)

	ConfigCMD.AddCommand(ConfigInitCMD)

	cobra.OnInitialize(onInit)
}

func onInit() {
	var err error
	conf, err = config.LoadConfig("")
	if err != nil {
		fmt.Fprintln(errWriter, "Can`t load config, use localhost:9092")
	}

	if clusterOverride != "" {
		conf.CurrentCluster = clusterOverride
	}

	// if config not inited
	if len(conf.Clusters) == 0 {
		conf.CurrentCluster = "local"
		conf.Clusters = append(conf.Clusters, config.Cluster{
			Name:    "local",
			Brokers: []string{"localhost:9092"},
		})
	}
}

var ConfigCMD = &cobra.Command{
	Use:   "config",
	Short: "Handle kafman configuration",
}

var ConfigCurrentContextCMD = &cobra.Command{
	Use:   "current-context",
	Short: "Displays the current context",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "%s", conf.CurrentCluster)
	},
}

var ConfigSelectCluster = &cobra.Command{
	Use:     "select-cluster",
	Aliases: []string{"ls"},
	Short:   "Interactively select a cluster",
	Run: func(cmd *cobra.Command, args []string) {
		var clusterNames []string
		var pos = 0
		for k, cluster := range conf.Clusters {
			clusterNames = append(clusterNames, cluster.Name)
			if cluster.Name == conf.GetCurrentCluster().Name {
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

		conf.SetCurrentCluster(selected)
		err = config.SaveConfig(conf, configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Can`t save config: %+v", err)
			os.Exit(0)
		}
	},
}

var ConfigInitCMD = &cobra.Command{
	Use:   "init",
	Short: "Create empty config and export to file (default ~/.kafeman/config.yml)",
	Run: func(cmd *cobra.Command, args []string) {
		err := config.ExportConfig(configPath)
		if err != nil {
			errorExit("Can`t save config: %+v", err)
		}

		fmt.Fprintf(os.Stdout, "Config created in ~/.config/kafeman/config.yml\n")
	},
}
