package kafeman_cmd

import (
	"fmt"
	"io"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/logger"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
)

var (

	// nolint
	colorableOut io.Writer = colorable.NewColorableStdout()

	commit  string = "HEAD"
	version string = "latest"
)

func newKafemanOptions() *kafemanOptions {
	return &kafemanOptions{
		configPath:      "~/.kafeman/config.yml",
		clusterOverride: "",
		failTolerance:   false,
		quiet:           false,
	}
}

type kafemanOptions struct {
	configPath      string
	clusterOverride string
	failTolerance   bool
	quiet           bool
}

func NewKafemanCMD() *cobra.Command {
	options := newKafemanOptions()
	cmd := &cobra.Command{
		Use:     "kafeman",
		Short:   "Kafka Command Line utility",
		Version: fmt.Sprintf("%s (%s)", version, commit),
	}

	cmd.PersistentFlags().StringVar(&options.configPath, "config", "", "config file (default is $HOME/.kafeman/config.yml)")
	cmd.PersistentFlags().StringVarP(&options.clusterOverride, "cluster", "c", "", "set a temporary current cluster")
	cmd.PersistentFlags().BoolVar(&options.failTolerance, "tolerance", false, "don't crash on errors")
	cmd.PersistentFlags().BoolVar(&options.quiet, "quiet", false, "do not print info and errors")
	cmd.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion())

	if options.clusterOverride != "" {
		config.Config.CurrentCluster = options.clusterOverride
	}

	fmt.Println("cluster: ", options.clusterOverride)

	logger.InitLogger(options.failTolerance, options.quiet)

	return cmd
}
