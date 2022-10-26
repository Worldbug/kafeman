package command

import (
	"fmt"
	"io"
	"os"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (
	outWriter io.Writer = os.Stdout
	errWriter io.Writer = os.Stderr
	inReader  io.Reader = os.Stdin

	colorableOut io.Writer = colorable.NewColorableStdout()

	commit  string = "HEAD"
	version string = "latest"
)

var RootCMD = &cobra.Command{
	Use:     "protokaf",
	Short:   "Kafka Command Line utility",
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
