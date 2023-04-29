package kafeman_cmd

import (
	"fmt"
	"io"

	"github.com/worldbug/kafeman/internal/config"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
)

var (

	// nolint
	colorableOut io.Writer = colorable.NewColorableStdout()

	commit  string = "HEAD"
	version string = "latest"
)

func newKafemanOptions() *kafemanOptions {
	return &kafemanOptions{}
}

type kafemanOptions struct {
}

func NewKafemanCMD() *cobra.Command {
	// options := newKafemanOptions()
	cmd := &cobra.Command{
		Use:     "kafeman",
		Short:   "Kafka Command Line utility",
		Version: fmt.Sprintf("%s (%s)", version, commit),
	}

	// TODO: указывать путь конфига
	// cmd.PersistentFlags().StringVar(&options.configPath, "config", "", "config file (default is $HOME/.kafeman/config.yml)")
	cmd.PersistentFlags().StringVarP(&config.Config.CurrentCluster, "cluster", "c", "", "set a temporary current cluster")
	viper.BindPFlag("current_cluster", cmd.PersistentFlags().Lookup("cluster"))
	cmd.PersistentFlags().BoolVar(&config.Config.FailTolerance, "tolerance", false, "don't crash on errors")
	viper.BindPFlag("fail_tolerance", cmd.PersistentFlags().Lookup("tolerance"))
	cmd.PersistentFlags().BoolVar(&config.Config.Quiet, "quiet", false, "do not print info and errors")
	viper.BindPFlag("quiet", cmd.PersistentFlags().Lookup("Quiet"))
	cmd.RegisterFlagCompletionFunc("cluster", completion_cmd.NewClusterCompletion())

	return cmd
}
