package command

import (
	"kafeman/internal/kafeman"
	"kafeman/internal/proto"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

var (
	protoFiles   []string
	protoExclude []string
	protoType    string

	offsetFlag     string
	groupIDFlag    string
	partitionsFlag []int32
	followFlag     bool
	commitFlag     bool
	printMetaFlag  bool
)

func init() {
	RootCMD.AddCommand(ConsumeCMD)

	ConsumeCMD.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer")
	ConsumeCMD.Flags().StringVarP(&groupIDFlag, "group", "g", "", "Consumer Group ID to use for consume")
	ConsumeCMD.Flags().BoolVarP(&followFlag, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	ConsumeCMD.Flags().BoolVar(&commitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	ConsumeCMD.Flags().BoolVar(&printMetaFlag, "meta", false, "Print with meta info (marshal into json)")
	ConsumeCMD.Flags().Int32SliceVarP(&partitionsFlag, "partitions", "p", []int32{}, "Partitions to consume")

}

var ConsumeCMD = &cobra.Command{
	Use:               "consume",
	Short:             "Consume messages from kafka topic",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		var offset int64
		topic := args[0]

		switch offsetFlag {
		case "oldest":
			offset = sarama.OffsetOldest
		case "newest":
			offset = sarama.OffsetNewest
		default:
			o, err := strconv.ParseInt(offsetFlag, 10, 64)
			if err != nil {
				errorExit("Could not parse '%s' to int64: %w", offsetFlag, err)
			}
			offset = o
		}

		pk := kafeman.Newkafeman(conf, outWriter, errWriter)
		pk.Consume(cmd.Context(), kafeman.ConsumeCommand{
			Topic:         topic,
			ConsumerGroup: groupIDFlag,
			Partitions:    partitionsFlag,
			Offset:        offset,
			MarkMessages:  commitFlag,
			Follow:        followFlag,
			WithMeta:      printMetaFlag,
		})
	},
}

func validTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	admin := getClusterAdmin()

	topics, err := admin.ListTopics()
	if err != nil {
		errorExit("Unable to list topics: %v\n", err)
	}
	topicList := make([]string, 0, len(topics))
	for topic := range topics {
		topicList = append(topicList, topic)
	}
	return topicList, cobra.ShellCompDirectiveNoFileComp
}

func getClientFromConfig(config *sarama.Config) (client sarama.Client) {
	client, err := sarama.NewClient(conf.GetCurrentCluster().Brokers, config)
	if err != nil {
		errorExit("Unable to get client: %v\n", err)
	}
	return client
}

var setupProtoDescriptorRegistry = func(cmd *cobra.Command, args []string) {
	if protoType != "" {
		r, err := proto.NewDescriptorRegistry(protoFiles, protoExclude)
		if err != nil {
			errorExit("Failed to load protobuf files: %v\n", err)
		}

		protoRegistry = r
	}
}

func getClusterAdmin() (admin sarama.ClusterAdmin) {
	clusterAdmin, err := sarama.NewClusterAdmin(conf.GetCurrentCluster().Brokers, getConfig())
	if err != nil {
		errorExit("Unable to get cluster admin: %v\n", err)
	}

	return clusterAdmin
}

func getConfig() (saramaConfig *sarama.Config) {
	saramaConfig = sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true
	return saramaConfig
}
