package command

import (
	"fmt"
	"kafeman/internal/config"
	"os"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

var (
	configPath = ""
)

func init() {
	RootCMD.AddCommand(ConfigCMD)
	RootCMD.PersistentFlags().StringVar(&configPath, "config", "", "config file (default is $HOME/.kafeman/config.yml)")

	// ConfigCMD.AddCommand(configImportCmd)
	// ConfigCMD.AddCommand(configUseCmd)
	// ConfigCMD.AddCommand(configLsCmd)
	// ConfigCMD.AddCommand(configAddClusterCmd)
	// ConfigCMD.AddCommand(configRemoveClusterCmd)
	ConfigCMD.AddCommand(ConfigSelectCluster)
	ConfigCMD.AddCommand(ConfigCurrentContextCMD)
	// ConfigCMD.AddCommand(configAddEventhub)

	ConfigCMD.AddCommand(ConfigInitCMD)

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
		fmt.Println(conf.CurrentCluster)
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
			os.Exit(0)
		}

		conf.SetCurrentCluster(selected)
		config.SaveConfig(conf, configPath)
	},
}

var ConfigInitCMD = &cobra.Command{
	Use:   "init",
	Short: "Create empty config and export to file (default ~/.kafeman/config.yml)",
	Run: func(cmd *cobra.Command, args []string) {
		config.ExportConfig(configPath)
	},
}
