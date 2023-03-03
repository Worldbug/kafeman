package kafeman_cmd

import (
	"fmt"
	"io"

	"github.com/worldbug/kafeman/internal/config"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (

	// nolint
	colorableOut io.Writer = colorable.NewColorableStdout()

	commit  string = "HEAD"
	version string = "latest"
)

func NewKafemanCMD(config config.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "kafeman",
		Short:   "Kafka Command Line utility",
		Version: fmt.Sprintf("%s (%s)", version, commit),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// TODO: remove ?
			// outWriter = cmd.OutOrStdout()
			// errWriter = cmd.ErrOrStderr()

			// if outWriter != os.Stdout {
			// 	colorableOut = outWriter
			// }
		},
	}
}
