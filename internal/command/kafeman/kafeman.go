package kafeman_cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/command/global_config"
)

var (
	commit  string = "HEAD"
	version string = "latest"
)

func NewKafemanCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "kafeman",
		Short:   "Kafka Command Line utility",
		Version: fmt.Sprintf("%s (%s)", version, commit),
	}

	// cmd.PersistentFlags().StringVar(&config.ConfigPath, "config", "", "config file (default is $HOME/.kafeman/config.yaml)")
	cmd.PersistentFlags().StringVarP(&global_config.Config.CurrentCluster, "cluster", "c", global_config.GetCurrentCluster().Name, "set a temporary current cluster")
	cmd.PersistentFlags().BoolVar(&global_config.Config.FailTolerance, "tolerance", false, "don't crash on errors")
	cmd.PersistentFlags().BoolVar(&global_config.Config.Quiet, "quiet", false, "do not print info and errors")
	cmd.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion())

	return cmd
}
