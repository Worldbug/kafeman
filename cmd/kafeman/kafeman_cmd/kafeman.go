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
	options := newKafemanOptions()

	cmd := &cobra.Command{
		Use:              "kafeman",
		Short:            "Kafka Command Line utility",
		Version:          fmt.Sprintf("%s (%s)", version, commit),
		PersistentPreRun: options.preRun,
	}

	cmd.PersistentFlags().StringVar(&options.ConfigPath, "config", run_configuration.GetDefaultConfigPath(), "set a temporary kafeman config file")
	cmd.PersistentFlags().StringVarP(&options.CurrentCluster, "cluster", "c", run_configuration.GetCurrentCluster().Name, "set a temporary current cluster")
	cmd.PersistentFlags().BoolVar(&options.FailTolerance, "tolerance", false, "don't crash on errors")
	cmd.PersistentFlags().BoolVar(&options.Quiet, "quiet", false, "do not print info and errors")
	cmd.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion())

	return cmd
}

func newKafemanOptions() *kafemanOptions {
	return &kafemanOptions{
		FailTolerance:  false,
		Quiet:          false,
		CurrentCluster: run_configuration.GetCurrentCluster().Name,
		ConfigPath:     run_configuration.GetDefaultConfigPath(),
	}
}

type kafemanOptions struct {
	FailTolerance  bool
	Quiet          bool
	CurrentCluster string
	ConfigPath     string
}

func (options *kafemanOptions) preRun(cmd *cobra.Command, args []string) {
	logger.FailTolerance = options.FailTolerance
	logger.Quiet = options.Quiet

	if options.ConfigPath != run_configuration.GetDefaultConfigPath() {
		run_configuration.ConfigPath = options.ConfigPath
		run_configuration.ReadConfiguration()
	}

	if options.CurrentCluster != run_configuration.GetCurrentCluster().Name {
		run_configuration.SetCurrentCluster(options.CurrentCluster)
	}
}
