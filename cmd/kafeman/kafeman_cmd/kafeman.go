package kafeman_cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/logger"
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

	cmd.PersistentFlags().StringVarP(&run_configuration.Config.CurrentCluster, "cluster", "c", run_configuration.GetCurrentCluster().Name, "set a temporary current cluster")
	cmd.PersistentFlags().BoolVar(&logger.FailTolerance, "tolerance", false, "don't crash on errors")
	cmd.PersistentFlags().BoolVar(&logger.Quiet, "quiet", false, "do not print info and errors")
	cmd.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion())

	return cmd
}
