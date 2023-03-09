package replicate_cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"
)

func newReplicateOptions(config *config.Config) *replicateOptions {
	return &replicateOptions{
		config: config,
	}
}

type replicateOptions struct {
	config *config.Config

	// TODO: refactor
	protoFiles    []string
	protoExclude  []string
	protoType     string
	protoRegistry *serializers.DescriptorRegistry

	offset        string
	groupID       string
	partition     int32
	partitions    []int32
	partitioner   string
	follow        bool
	commit        bool
	printMeta     bool
	fromAt        string
	messagesCount int32

	encoding string
}

func (r *replicateOptions) run(cmd *cobra.Command, args []string) {
	offset := command.GetOffsetFromFlag(r.offset)
	source := parseReplicateArg(args[0])
	dest := parseReplicateArg(args[1])

	k := kafeman.Newkafeman(r.config)
	k.Replicate(cmd.Context(), kafeman.ReplicateCMD{
		SourceTopic:    source[1],
		SourceBroker:   source[0],
		DestTopic:      dest[1],
		DestBroker:     dest[0],
		Partition:      r.partition,
		Partitioner:    r.partitioner,
		ConsumerGroup:  r.groupID,
		Partitions:     r.partitions,
		Offset:         offset,
		CommitMessages: r.commit,
		Follow:         r.follow,
		WithMeta:       r.printMeta,
		MessagesCount:  r.messagesCount,
		FromTime:       command.ParseTime(r.fromAt),
	})

}
func (r *replicateOptions) setupProtoDescriptorRegistry(cmd *cobra.Command, args []string) {
	if r.protoType != "" {
		reg, err := serializers.NewDescriptorRegistry(r.protoFiles, r.protoExclude)
		if err != nil {
			command.ExitWithErr("Failed to load protobuf files: %v\n", err)
		}

		r.protoRegistry = reg
	}
}

// kafeman replicate prod/events local/events
func NewReplicateCMD(config *config.Config) *cobra.Command {
	options := newReplicateOptions(config)

	cmd := &cobra.Command{
		Use:               "replicate [source] [dest]",
		Short:             "Replicate messages from source topic to destination topic",
		Example:           "kafeman replicate prod_cluster/topic_name local_cluster/topic_name",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completion_cmd.NewReplicationCompletion(config),
		PreRun:            options.setupProtoDescriptorRegistry,
		Run:               options.run,
	}

	cmd.Flags().StringVar(&options.offset, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	cmd.RegisterFlagCompletionFunc("offset", completion_cmd.NewOffsetCompletion())
	cmd.Flags().StringVar(&options.encoding, "force-encoding", "", "Fore set encoding one of [raw,proto,avro,msgpack,base64]")
	cmd.RegisterFlagCompletionFunc("force-encoding", completion_cmd.NewEncodingCompletion())
	cmd.Flags().StringVarP(&options.groupID, "group", "g", "", "Consumer Group ID to use for consume")
	cmd.RegisterFlagCompletionFunc("group", completion_cmd.NewGroupCompletion(config))
	cmd.Flags().BoolVarP(&options.follow, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	cmd.Flags().BoolVar(&options.commit, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	cmd.Flags().BoolVar(&options.printMeta, "meta", false, "Print with meta info (marshal into json)")
	cmd.Flags().Int32SliceVarP(&options.partitions, "partitions", "p", []int32{}, "Partitions to consume")
	cmd.Flags().Int32VarP(&options.messagesCount, "tail", "n", 0, "Print last n messages per partition")
	cmd.Flags().StringVar(&options.fromAt, "from", "", "Consume messages earlier time (format 2022-10-30T00:00:00)")
	cmd.RegisterFlagCompletionFunc("from", completion_cmd.NewTimeCompletion())

	cmd.Flags().StringVar(&options.partitioner, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	cmd.RegisterFlagCompletionFunc("partitioner", completion_cmd.NewPartitionerCompletion())
	cmd.Flags().Int32VarP(&options.partition, "partition", "p", -1, "Partition to produce to")

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
