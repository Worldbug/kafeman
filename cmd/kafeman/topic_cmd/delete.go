package topic_cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func NewDeleteTopicCMD() *cobra.Command {
	options := newDeleteTopicOptions()

	cmd := &cobra.Command{
		Use:               "delete TOPIC",
		Short:             "Delete a topic",
		Args:              cobra.ExactArgs(1),
		Example:           "kafeman topic delete topic_name",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	return cmd
}

func newDeleteTopicOptions() *deleteTopicOptions {
	return &deleteTopicOptions{
		out: os.Stdout,
	}
}

type deleteTopicOptions struct {
	out io.Writer
}

func (d *deleteTopicOptions) run(cmd *cobra.Command, args []string) {
	topic := args[0]

	k := kafeman.Newkafeman(run_configuration.Config)
	err := k.DeleteTopic(cmd.Context(), topic)
	if err != nil {
		os.Exit(1)
	}

	fmt.Fprintf(d.out, "\xE2\x9C\x85 Deleted topic %v!\n", topic)
}
