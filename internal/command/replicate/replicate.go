package replicate_cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func newReplicateOptions() *replicateOptions {
	ret
}

type replicateOptions struct {
	config config.Config

	offsetFlag        string
	groupIDFlag       string
	partitionsFlag    []int32
	followFlag        bool
	commitFlag        bool
	printMetaFlag     bool
	fromAtFlag        string
	messagesCountFlag int32

	encoding string
}

func (ro *replicateOptions) run(cmd *cobra.Command, args []string) {
	offset := getOffsetFromFlag()
	source := parseReplicateArg(args[0])
	dest := parseReplicateArg(args[1])

	k := kafeman.Newkafeman(config)
	k.Replicate(cmd.Context(), kafeman.ReplicateCMD{
		SourceTopic:    source[1],
		SourceBroker:   source[0],
		DestTopic:      dest[1],
		DestBroker:     dest[0],
		Partition:      ro.partitionFlag,
		Partitioner:    ro.partitionerFlag,
		ConsumerGroup:  ro.groupIDFlag,
		Partitions:     ro.partitionsFlag,
		Offset:         ro.offset,
		CommitMessages: ro.commitFlag,
		Follow:         ro.followFlag,
		WithMeta:       ro.printMetaFlag,
		MessagesCount:  ro.messagesCountFlag,
		FromTime:       parseTime(fromAtFlag),
	})

}

// kafeman replicate prod/events local/events
func NewReplicateCMD(config config.Config) *cobra.Command {
	o := replicateOptions{}

	cmd := &cobra.Command{
		Use:               "replicate [source] [dest]",
		Short:             "Replicate messages from source topic to destination topic",
		Example:           "kafeman replicate prod_cluster/topic_name local_cluster/topic_name",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: replicationCompletion,
		PreRun:            setupProtoDescriptorRegistry,
		Run:               o.run,
	}

	cmd.Flags().StringVar(&o.offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	cmd.RegisterFlagCompletionFunc("offset", offsetCompletion)
	cmd.Flags().StringVar(&o.groupIDFlag, "group", "", "Consumer Group ID to use for consume")
	cmd.RegisterFlagCompletionFunc("group", groupCompletion)
	cmd.Flags().BoolVar(&o.followFlag, "follow", false, "Continue to consume messages until program execution is interrupted/terminated")
	cmd.Flags().BoolVar(&o.commitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	cmd.Flags().Int32SliceVar(&o.partitionsFlag, "partitions", []int32{}, "Partitions to consume")
	cmd.Flags().Int32Var(&o.messagesCountFlag, "tail", 0, "Print last n messages per partition")
	cmd.Flags().StringVar(&o.fromAtFlag, "from", "", "Consume messages earlier time (format 2022-10-30T00:00:00)")
	cmd.RegisterFlagCompletionFunc("from", timeCompletion)
	cmd.Flags().StringVar(&o.partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	cmd.RegisterFlagCompletionFunc("partitioner", partitionerCompletion)
	cmd.Flags().Int32Var(&o.partitionFlag, "partition", -1, "Partition to produce to")

	return cmd
}

// cluster/topic -> []string{cluster, topic}
func parseReplicateArg(arg string) []string {
	args := strings.Split(arg, "/")
	if len(args) == 2 {
		return args
	}

	if len(args) == 1 {
		return []string{args[0], ""}
	}

	return []string{"", ""}
}
