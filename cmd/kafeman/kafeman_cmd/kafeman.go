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

	cmd.PersistentFlags().StringVar(&options.configPath, "config", run_configuration.GetDefaultConfigPath(), "set a temporary kafeman config file")
	cmd.PersistentFlags().StringVarP(&options.currentCluster, "cluster", "c", run_configuration.GetCurrentCluster().Name, "set a temporary current cluster")
	cmd.PersistentFlags().BoolVar(&options.failTolerance, "tolerance", false, "don't crash on errors")
	cmd.PersistentFlags().BoolVar(&options.quiet, "quiet", false, "do not print info and errors")
	cmd.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion())

	return cmd
}

func newKafemanOptions() *kafemanOptions {
	return &kafemanOptions{
		failTolerance:  false,
		quiet:          false,
		currentCluster: run_configuration.GetCurrentCluster().Name,
		configPath:     run_configuration.GetDefaultConfigPath(),
	}
}

type kafemanOptions struct {
	failTolerance  bool
	quiet          bool
	currentCluster string
	configPath     string
}

func (options *kafemanOptions) preRun(cmd *cobra.Command, args []string) {
	logger.FailTolerance = options.failTolerance
	logger.Quiet = options.quiet

	if options.configPath != run_configuration.GetDefaultConfigPath() {
		run_configuration.ConfigPath = options.configPath
		run_configuration.ReadConfiguration()
	}

	if options.currentCluster != run_configuration.GetCurrentCluster().Name {
		run_configuration.SetCurrentCluster(options.currentCluster)
	}
}
