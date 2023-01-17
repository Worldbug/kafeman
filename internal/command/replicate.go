package command

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func init() {
	RootCMD.AddCommand(replicateCMD)
	replicateCMD.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	replicateCMD.RegisterFlagCompletionFunc("offset", offsetCompletion)
	replicateCMD.Flags().StringVar(&groupIDFlag, "group", "", "Consumer Group ID to use for consume")
	replicateCMD.RegisterFlagCompletionFunc("group", groupCompletion)
	replicateCMD.Flags().BoolVar(&followFlag, "follow", false, "Continue to consume messages until program execution is interrupted/terminated")
	replicateCMD.Flags().BoolVar(&commitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	replicateCMD.Flags().Int32SliceVar(&partitionsFlag, "partitions", []int32{}, "Partitions to consume")
	replicateCMD.Flags().Int32Var(&messagesCountFlag, "tail", 0, "Print last n messages per partition")
	replicateCMD.Flags().StringVar(&fromAtFlag, "from", "", "Consume messages earlier time (format 2022-10-30T00:00:00)")
	replicateCMD.RegisterFlagCompletionFunc("from", timeCompletion)
	replicateCMD.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	replicateCMD.RegisterFlagCompletionFunc("partitioner", partitionerCompletion)
	replicateCMD.Flags().Int32Var(&partitionFlag, "partition", -1, "Partition to produce to")
}

// kafeman replicate prod/events local/events
var replicateCMD = &cobra.Command{
	Use:               "replicate [source] [dest]",
	Short:             "Replicate messages from source topic to destination topic",
	Long:              "replicate prod/topic local/topic",
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: replicationCompletion,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		offset := getOffsetFromFlag()
		source := parseReplicateArg(args[0])
		dest := parseReplicateArg(args[1])

		k := kafeman.Newkafeman(conf)
		k.Replicate(cmd.Context(), kafeman.ReplicateCMD{
			SourceTopic:    source[1],
			SourceBroker:   source[0],
			DestTopic:      dest[1],
			DestBroker:     dest[0],
			Partition:      partitionFlag,
			Partitioner:    partitionerFlag,
			ConsumerGroup:  groupIDFlag,
			Partitions:     partitionsFlag,
			Offset:         offset,
			CommitMessages: commitFlag,
			Follow:         followFlag,
			WithMeta:       printMetaFlag,
			MessagesCount:  messagesCountFlag,
			FromTime:       parseTime(fromAtFlag),
		})

	},
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
