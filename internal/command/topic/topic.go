package topic_cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/logger"
	"github.com/worldbug/kafeman/internal/models"

	"github.com/spf13/cobra"
)

var (
	partitionAssignmentsFlag string
	// TODO: delete
	replicasFlag int16
	noHeaderFlag bool
	compactFlag  bool
)

func init() {
	RootCMD.AddCommand(TopicCMD)
	RootCMD.AddCommand(TopicsCMD)

	TopicCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")
	TopicsCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")

	TopicCMD.AddCommand(DescribeCMD)
	TopicCMD.AddCommand(TopicConsumersCMD)
	TopicCMD.AddCommand(deleteTopicCMD)
	DescribeCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")
	TopicCMD.AddCommand(LsTopicsCMD)
	TopicCMD.AddCommand(createTopicCmd)
	TopicCMD.AddCommand(addConfigCmd)
	TopicCMD.AddCommand(topicSetConfig)
	TopicCMD.AddCommand(updateTopicCmd)

	createTopicCmd.Flags().Int32VarP(&partitionFlag, "partitions", "p", int32(1), "Number of partitions")
	createTopicCmd.Flags().Int16VarP(&replicasFlag, "replicas", "r", int16(1), "Number of replicas")
	createTopicCmd.Flags().BoolVar(&compactFlag, "compact", false, "Enable topic compaction")

	LsTopicsCMD.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	TopicsCMD.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	updateTopicCmd.Flags().Int32VarP(&partitionFlag, "partitions", "p", int32(-1), "Number of partitions")
	updateTopicCmd.Flags().StringVar(&partitionAssignmentsFlag, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")
}

func NewTopicCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Create and describe topics.",
	}

	return cmd
}

func newDescribeOptions(config config.Config) *describeOptions {
	return &describeOptions{
		config:           config,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
	}
}

func NewDescribeCMD(config config.Config) *cobra.Command {
	options := newDescribeOptions(config)

	cmd := &cobra.Command{
		Use:               "describe",
		Short:             "Describe topic info",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(config),
		Run:               options.run,
	}

	return cmd
}

type describeOptions struct {
	config config.Config
	command.PrettyPrintFlags
	out    io.Writer
	asJson bool
}

func (d *describeOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(d.config)
	topicInfo, err := k.DescribeTopic(cmd.Context(), args[0])
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	if d.asJson {
		d.describeTopicPrintJson(topicInfo)
		return
	}

	d.describeTopicPrint(topicInfo)
}

func (d *describeOptions) describeTopicPrintJson(topicInfo models.TopicInfo) {
	raw, err := json.Marshal(topicInfo)
	if err != nil {
		return
	}

	fmt.Fprintln(d.out, string(raw))
}

func (d *describeOptions) describeTopicPrint(topicInfo models.TopicInfo) {
	w := tabwriter.NewWriter(d.out, d.MinWidth, d.Width, d.Padding, d.PadChar, d.Flags)
	defer w.Flush()

	fmt.Fprintf(w, "Topic:\t%s\n", topicInfo.TopicName)
	w.Flush()

	fmt.Fprintf(w, "Partitions:\n")
	w.Flush()

	if !noHeaderFlag {
		fmt.Fprintf(w, "\tPartition\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t--------------\t------\t--------\t---\t\n")
	}
	sort.Slice(topicInfo.Partitions, func(i, j int) bool {
		return topicInfo.Partitions[i].Partition < topicInfo.Partitions[j].Partition
	})
	for _, p := range topicInfo.Partitions {
		fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t\n", p.Partition, p.HightWatermark, p.Leader, p.Replicas, p.ISR)
	}
	w.Flush()

	if !noHeaderFlag {
		fmt.Fprintf(w, "Config:\n")
		fmt.Fprintf(w, "\tKey\tValue\tRead only\tSensitive\n")
		fmt.Fprintf(w, "\t---\t-----\t---------\t---------\n")
	}

	for _, c := range topicInfo.Config {
		fmt.Fprintf(w, "\t%s\t%s\t%v\t%v\n", c.Name, c.Value, c.ReadOnly, c.Sensitive)
	}

}

func NewTopicsCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List topics",
		Run:   LsTopicsCMD.Run,
	}

	return cmd
}

func NewLSTopicsCMD(config config.Config) *cobra.Command {
	options := newLSTopicsOptions(config)
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		Run:     options.run,
	}

	return cmd
}

func newLSTopicsOptions(config config.Config) *lsTopicsOption {
	return &lsTopicsOption{
		config: config,
	}
}

type lsTopicsOption struct {
	config config.Config
}

func (l *lsTopicsOption) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(l.config)

	topics, err := k.ListTopics(cmd.Context())
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	if l.asJson {
		command.PrintJson(topics)
		return
	}

	l.lsTopicsPrint(topics)
}

func (l *lsTopicsOption) lsTopicsPrint(topics []models.Topic) {
	w := newTabWriter()

	if !noHeaderFlag {
		fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
	}

	for _, topic := range topics {
		fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.Name, topic.Partitions, topic.Replicas)
	}
	w.Flush()
}

var TopicConsumersCMD = &cobra.Command{
	Use:               "consumers",
	Short:             "List topic consumers",
	Args:              cobra.ExactArgs(1),
	Example:           "kafeman topic consumers topic_name",
	ValidArgsFunction: topicCompletion,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		consumers, err := k.ListTopicConsumers(cmd.Context(), args[0])
		if err != nil {
			errorExit("%+v", err)
		}

		if asJsonFlag {
			printJson(consumers)
			return
		}

		topicConsumersPrint(consumers)
	},
}

func topicConsumersPrint(consumers models.TopicConsumers) {
	w := newTabWriter()
	defer w.Flush()

	if !noHeaderFlag {
		fmt.Fprintf(w, "Consumers:\n")
		fmt.Fprintf(w, "\tName\tMembers\n")
		fmt.Fprintf(w, "\t----\t-------\n")
	}
	for _, c := range consumers.Consumers {
		fmt.Fprintf(w, "\t%s\t%d\n", c.Name, c.MembersCount)
	}
}

var deleteTopicCMD = &cobra.Command{
	Use:               "delete TOPIC",
	Short:             "Delete a topic",
	Args:              cobra.ExactArgs(1),
	Example:           "kafeman topic delete topic_name",
	ValidArgsFunction: topicCompletion,
	Run: func(cmd *cobra.Command, args []string) {
		topic := args[0]

		k := kafeman.Newkafeman(conf)
		err := k.DeleteTopic(cmd.Context(), topic)
		if err != nil {
			os.Exit(1)
		}

		fmt.Fprintf(outWriter, "\xE2\x9C\x85 Deleted topic %v!\n", topic)
	},
}

var topicSetConfig = &cobra.Command{
	Use:               "set-config",
	Short:             "set topic config. requires Kafka >=2.3.0 on broker side and kafeman cluster config.",
	Example:           "kafeman topic set-config topic.name cleanup.policy=delete",
	Args:              cobra.ExactArgs(2),
	ValidArgsFunction: topicCompletion,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)

		topic := args[0]

		splt := strings.Split(args[1], ",")
		configs := make(map[string]string)

		for _, kv := range splt {
			s := strings.Split(kv, "=")

			if len(s) != 2 {
				continue
			}

			configs[s[0]] = s[1]
		}

		if len(configs) < 1 {
			logger.Errorf("No valid configs found")
		}

		err := k.SetConfigValueTopic(cmd.Context(), kafeman.SetConfigValueTopicCommand{
			Topic:  topic,
			Values: configs,
		})
		if err != nil {
			os.Exit(1)
		}

		fmt.Printf("\xE2\x9C\x85 Updated config.")
	},
}

var updateTopicCmd = &cobra.Command{
	Use:               "update",
	Short:             "Update topic",
	Example:           "kafeman topic update topic_name -p 5 --partition-assignments '[[1,2,3],[1,2,3]]'",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: topicCompletion,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		topic := args[0]

		if partitionFlag == -1 && partitionAssignmentsFlag == "" {
			errorExit("Number of partitions and/or partition assignments must be given")
		}

		var assignments [][]int32
		if partitionAssignmentsFlag != "" {
			if err := json.Unmarshal([]byte(partitionAssignmentsFlag), &assignments); err != nil {
				errorExit("Invalid partition assignments: %v", err)
			}
		}

		err := k.UpdateTopic(cmd.Context(), kafeman.UpdateTopicCommand{
			Topic:           topic,
			PartitionsCount: partitionFlag,
			Assignments:     assignments,
		})
		if err != nil {
			os.Exit(1)
		}

		fmt.Printf("\xE2\x9C\x85 Updated topic!\n")
	},
}

var createTopicCmd = &cobra.Command{
	Use:     "create TOPIC",
	Short:   "Create a topic",
	Example: "kafeman topic create topic_name --partitions 6",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		topic := args[0]

		cleanupPolicy := "delete"
		if compactFlag {
			cleanupPolicy = "compact"
		}

		err := k.CreateTopic(cmd.Context(), kafeman.CreateTopicCommand{
			TopicName:         topic,
			PartitionsCount:   partitionFlag,
			ReplicationFactor: replicasFlag,
			CleanupPolicy:     cleanupPolicy,
		})
		if err != nil {
			errorExit("Could not create topic %v: %v\n", topic, err.Error())
		}

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
		fmt.Fprintf(w, "\xE2\x9C\x85 Created topic!\n")
		fmt.Fprintln(w, "\tTopic Name:\t", topic)
		fmt.Fprintln(w, "\tPartitions:\t", partitionsFlag)
		fmt.Fprintln(w, "\tReplication Factor:\t", replicasFlag)
		fmt.Fprintln(w, "\tCleanup Policy:\t", cleanupPolicy)
		w.Flush()
	},
}

var addConfigCmd = &cobra.Command{
	Use:               "add-config TOPIC KEY VALUE",
	Short:             "Add config key/value pair to topic",
	Example:           "kafeman topic add-config topic_name compression.type gzip",
	ValidArgsFunction: topicCompletion,
	Args:              cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)

		topic := args[0]
		key := args[1]
		value := args[2]

		err := k.AddConfigRecord(cmd.Context(), kafeman.AddConfigRecordCommand{
			Topic: topic,
			Key:   key,
			Value: value,
		})
		if err != nil {
			os.Exit(1)
		}

		fmt.Printf("Added config %v=%v to topic %v.\n", key, value, topic)
	},
}
