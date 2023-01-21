package command

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (
	conf config.Config

	outWriter io.Writer = os.Stdout
	errWriter io.Writer = os.Stderr

	// nolint
	colorableOut io.Writer = colorable.NewColorableStdout()

	commit  string = "HEAD"
	version string = "latest"

	protoRegistry *serializers.DescriptorRegistry
)

var RootCMD = &cobra.Command{
	Use:     "kafeman",
	Short:   "Kafka Command Line utility",
	Version: fmt.Sprintf("%s (%s)", version, commit),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		outWriter = cmd.OutOrStdout()
		errWriter = cmd.ErrOrStderr()

		if outWriter != os.Stdout {
			colorableOut = outWriter
		}
	},
}

func errorExit(format string, a ...interface{}) {
	fmt.Fprintf(errWriter, format+"\n", a...)
	os.Exit(1)
}

// nolint
func inTTY() bool {
	fi, _ := os.Stdout.Stat()
	return fi.Mode()&os.ModeCharDevice != 0
}

// TODO: перенести все на этот метод
func printJson(model any) {
	raw, err := json.Marshal(model)
	if err != nil {
		errorExit("%+v", err)
	}

	fmt.Fprintln(outWriter, string(raw))
}
