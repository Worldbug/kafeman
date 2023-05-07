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
)

func NewCreateTopicCmd() *cobra.Command {
	options := newCreateTopicOptions()
	cmd := &cobra.Command{
		Use:     "create TOPIC",
		Short:   "Create a topic",
		Example: "kafeman topic create topic_name --partitions 6",
		Args:    cobra.ExactArgs(1),
		Run:     options.run,
	}

	cmd.Flags().Int32VarP(&options.partitions, "partitions", "p", int32(1), "Number of partitions")
	cmd.Flags().Int16VarP(&options.replicas, "replicas", "r", int16(1), "Number of replicas")
	cmd.Flags().BoolVar(&options.compact, "compact", false, "Enable topic compaction")

	return cmd
}

func newCreateTopicOptions() *createTopicOptions {
	return &createTopicOptions{
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
		out:              os.Stdout,
	}
}

type createTopicOptions struct {
	common.PrettyPrintFlags
	out io.Writer

	// TODO:
	noHeader   bool
	compact    bool
	replicas   int16
	partitions int32
}

func (c *createTopicOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	topic := args[0]

	cleanupPolicy := "delete"
	if c.compact {
		cleanupPolicy = "compact"
	}

	err := k.CreateTopic(cmd.Context(), kafeman.CreateTopicCommand{
		TopicName:         topic,
		PartitionsCount:   c.partitions,
		ReplicationFactor: c.replicas,
		CleanupPolicy:     cleanupPolicy,
	})
	if err != nil {
		common.ExitWithErr("Could not create topic %v: %v\n", topic, err.Error())
	}
	w := tabwriter.NewWriter(c.out, c.MinWidth, c.Width, c.Padding, c.PadChar, c.Flags)
	defer w.Flush()

	fmt.Fprintf(w, "\xE2\x9C\x85 Created topic!\n")
	fmt.Fprintln(w, "\tTopic Name:\t", topic)
	fmt.Fprintln(w, "\tPartitions:\t", c.partitions)
	fmt.Fprintln(w, "\tReplication Factor:\t", c.replicas)
	fmt.Fprintln(w, "\tCleanup Policy:\t", cleanupPolicy)
}
