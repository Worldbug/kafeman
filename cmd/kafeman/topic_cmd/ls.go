package topic_cmd

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
)

func NewLSTopicsCMD() *cobra.Command {
	options := newLSTopicsOptions()

	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		Run:     options.run,
	}

	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")
	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

func newLSTopicsOptions() *lsTopicsOptions {
	return &lsTopicsOptions{
		out:              os.Stdout,
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
	}
}

type lsTopicsOptions struct {
	common.PrettyPrintFlags
	out    io.Writer
	asJson bool
}

func (l *lsTopicsOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)

	topics, err := k.ListTopics(cmd.Context())
	if err != nil {
		common.ExitWithErr("%+v", err)
	}

	if l.asJson {
		common.PrintJson(topics)
		return
	}

	l.lsTopicsPrint(topics)
}

func (l *lsTopicsOptions) lsTopicsPrint(topics []models.Topic) {
	w := tabwriter.NewWriter(l.out, l.MinWidth, l.Width, l.Padding, l.PadChar, l.Flags)
	w.Flush()

	if !l.NoHeader {
		fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
	}

	for _, topic := range topics {
		fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.Name, topic.Partitions, topic.Replicas)
	}
	w.Flush()
}
