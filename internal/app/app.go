package app

import (
	"fmt"
	"io"
	"os"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

func init() {

}

var (
	outWriter io.Writer = os.Stdout
	errWriter io.Writer = os.Stderr
	inReader  io.Reader = os.Stdin

	colorableOut io.Writer = colorable.NewColorableStdout()
)

var commit string = "HEAD"
var version string = "latest"

func Execute() error {
	return rootCMD.Execute()
}

var rootCMD = &cobra.Command{
	Use:     "gkat",
	Short:   "Kafka Command Line utility for cluster management",
	Version: fmt.Sprintf("%s (%s)", version, commit),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		outWriter = cmd.OutOrStdout()
		errWriter = cmd.ErrOrStderr()
		inReader = cmd.InOrStdin()

		if outWriter != os.Stdout {
			colorableOut = outWriter
		}
	},
}
