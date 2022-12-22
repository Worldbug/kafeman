package command

import (
	"encoding/json"
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"

	"github.com/spf13/cobra"
)

const (
	tabwriterMinWidth       = 6
	tabwriterMinWidthNested = 2
	tabwriterWidth          = 4
	tabwriterPadding        = 3
	tabwriterPadChar        = ' '
	tabwriterFlags          = 0
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

	TopicCMD.AddCommand(DescribeCMD)
	TopicCMD.AddCommand(TopicConsumersCMD)
	DescribeCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")
	// TopicCMD.AddCommand(createTopicCmd)
	// TopicCMD.AddCommand(deleteTopicCmd)
	TopicCMD.AddCommand(LsTopicsCMD)
	// TopicCMD.AddCommand(describeTopicCmd)
	// TopicCMD.AddCommand(addConfigCmd)
	// TopicCMD.AddCommand(topicSetConfig)
	// TopicCMD.AddCommand(updateTopicCmd)

	// createTopicCmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(1), "Number of partitions")
	// createTopicCmd.Flags().Int16VarP(&replicasFlag, "replicas", "r", int16(1), "Number of replicas")
	// createTopicCmd.Flags().BoolVar(&compactFlag, "compact", false, "Enable topic compaction")

	LsTopicsCMD.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	// topicsCmd.Flags().BoolVar(&noHeaderFlag, "no-headers", false, "Hide table headers")
	// updateTopicCmd.Flags().Int32VarP(&partitionsFlag, "partitions", "p", int32(-1), "Number of partitions")
	// updateTopicCmd.Flags().StringVar(&partitionAssignmentsFlag, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")
}

var TopicCMD = &cobra.Command{
	Use:   "topic",
	Short: "Create and describe topics.",
}

func newTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(
		outWriter, tabwriterMinWidth, tabwriterWidth,
		tabwriterPadding, tabwriterPadChar, tabwriterFlags,
	)
}

var DescribeCMD = &cobra.Command{
	Use:               "describe",
	Short:             "Describe topic info",
	ValidArgsFunction: topicCompletion,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		topicInfo, err := k.DescribeTopic(cmd.Context(), args[0])
		if err != nil {
			errorExit("%+v", err)
		}

		if asJsonFlag {
			describeTopicPrintJson(topicInfo)
			return
		}

		describeTopicPrint(topicInfo)
	},
}

func describeTopicPrintJson(topicInfo models.TopicInfo) {
	raw, err := json.Marshal(topicInfo)
	if err != nil {
		return
	}

	fmt.Fprintln(outWriter, string(raw))
}

func describeTopicPrint(topicInfo models.TopicInfo) {
	w := newTabWriter()
	defer w.Flush()

	fmt.Fprintf(w, "Topic:\t%s\n", topicInfo.TopicName)
	w.Flush()

	fmt.Fprintf(w, "Partition:\t%s\n", topicInfo.TopicName)
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

var TopicsCMD = &cobra.Command{
	Use:   "topics",
	Short: "List topics",
	Run:   LsTopicsCMD.Run,
}

var LsTopicsCMD = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List topics",
	Args:    cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		w := newTabWriter()

		if !noHeaderFlag {
			fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
		}

		topics, err := k.ListTopics(cmd.Context())
		if err != nil {
			errorExit("%+v", err)
		}

		for _, topic := range topics {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.Name, topic.Partitions, topic.Replicas)
		}
		w.Flush()
	},
}

var TopicConsumersCMD = &cobra.Command{
	Use:               "consumers",
	Short:             "List topic consumers",
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
