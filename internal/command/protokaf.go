package command

import (
	"fmt"
	"io"
	"os"

	"protokaf/internal/config"
	"protokaf/internal/proto"

	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

func init() {
	var err error
	conf, err = config.LoadConfig("")
	if err != nil {
		panic("Cant load config")
	}

	for _, v := range conf.Protobuf.ProtoPaths {
		protoFiles = append(protoFiles, v)
	}
}

var (
	conf config.Config
	// currentCluster config.Cluster

	outWriter io.Writer = os.Stdout
	errWriter io.Writer = os.Stderr
	inReader  io.Reader = os.Stdin

	colorableOut io.Writer = colorable.NewColorableStdout()

	commit  string = "HEAD"
	version string = "latest"

	protoRegistry *proto.DescriptorRegistry
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

func errorExit(format string, a ...interface{}) {
	fmt.Fprintf(errWriter, format+"\n", a...)
	os.Exit(1)
}
