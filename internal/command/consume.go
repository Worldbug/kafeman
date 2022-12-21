package command

import (
	"strconv"
	"time"

	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/spf13/cobra"
)

const (
	newestOffset = -1
	oldestOffset = -2
)

var (
	protoFiles   []string
	protoExclude []string
	protoType    string

	offsetFlag        string
	groupIDFlag       string
	partitionsFlag    []int32
	followFlag        bool
	commitFlag        bool
	printMetaFlag     bool
	fromAtFlag        string
	messagesCountFlag int32
)

func init() {
	RootCMD.AddCommand(ConsumeCMD)

	ConsumeCMD.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	ConsumeCMD.Flags().StringVarP(&groupIDFlag, "group", "g", "", "Consumer Group ID to use for consume")
	ConsumeCMD.Flags().BoolVarP(&followFlag, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	ConsumeCMD.Flags().BoolVar(&commitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	ConsumeCMD.Flags().BoolVar(&printMetaFlag, "meta", false, "Print with meta info (marshal into json)")
	ConsumeCMD.Flags().Int32SliceVarP(&partitionsFlag, "partitions", "p", []int32{}, "Partitions to consume")
	ConsumeCMD.Flags().Int32VarP(&messagesCountFlag, "tail", "n", 0, "Print last n messages per partition")
	ConsumeCMD.Flags().StringVar(&fromAtFlag, "from", "", "Consume messages earlier time (format 2022-10-30T00:00:00)")

}

var ConsumeCMD = &cobra.Command{
	Use:               "consume",
	Short:             "Consume messages from kafka topic",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		offset := getOffsetFromFlag()
		topic := args[0]

		k := kafeman.Newkafeman(conf)
		k.Consume(cmd.Context(), models.ConsumeCommand{
			Topic:          topic,
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

func parseTime(str string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05", str)
	if err != nil {
		return time.Unix(0, 0)
	}

	return t.UTC()
}

func validTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	k := kafeman.Newkafeman(conf)
	topics, err := k.ListTopics(cmd.Context())
	if err != nil {
		errorExit("%+v", err)
	}

	topicList := make([]string, 0, len(topics))
	for _, topic := range topics {
		topicList = append(topicList, topic.Name)
	}

	return topicList, cobra.ShellCompDirectiveNoFileComp
}

var setupProtoDescriptorRegistry = func(cmd *cobra.Command, args []string) {
	if protoType != "" {
		r, err := serializers.NewDescriptorRegistry(protoFiles, protoExclude)
		if err != nil {
			errorExit("Failed to load protobuf files: %v\n", err)
		}

		protoRegistry = r
	}
}

func getOffsetFromFlag() int64 {
	var offset int64
	switch offsetFlag {
	case "oldest":
		offset = oldestOffset
		break
	case "newest":
		offset = newestOffset
		break
	default:
		o, err := strconv.ParseInt(offsetFlag, 10, 64)
		if err != nil {
			errorExit("Could not parse '%s' to int64: %w", offsetFlag, err)
		}
		offset = o
	}

	return offset
}
