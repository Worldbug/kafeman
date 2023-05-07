package topic_cmd

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
)

func NewTopicConsumersCMD() *cobra.Command {
	options := newTopicConsumersOptions()

	cmd := &cobra.Command{
		Use:               "consumers",
		Short:             "List topic consumers",
		Args:              cobra.ExactArgs(1),
		Example:           "kafeman topic consumers topic_name",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")
	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

func newTopicConsumersOptions() *topicConsumersOptions {
	return &topicConsumersOptions{
		out:              os.Stdout,
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
	}
}

type topicConsumersOptions struct {
	asJson bool
	out    io.Writer

	common.PrettyPrintFlags
}

func (t *topicConsumersOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	consumers, err := k.ListTopicConsumers(cmd.Context(), args[0])
	if err != nil {
		common.ExitWithErr("%+v", err)
	}

	if t.asJson {
		common.PrintJson(consumers)
		return
	}

	t.topicConsumersPrint(consumers)
}

func (t *topicConsumersOptions) topicConsumersPrint(consumers models.TopicConsumers) {
	w := tabwriter.NewWriter(t.out, t.MinWidth, t.Width, t.Padding, t.PadChar, t.Flags)
	defer w.Flush()

	if !t.NoHeader {
		fmt.Fprintf(w, "Consumers:\n")
		fmt.Fprintf(w, "\tName\tMembers\n")
		fmt.Fprintf(w, "\t----\t-------\n")
	}
	for _, c := range consumers.Consumers {
		fmt.Fprintf(w, "\t%s\t%d\n", c.Name, c.MembersCount)
	}
}
