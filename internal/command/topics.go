package command

import (
	"encoding/json"
	"fmt"
	"kafeman/internal/kafeman"
	"kafeman/internal/models"
	"text/tabwriter"

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
	//
)

func init() {
	RootCMD.AddCommand(TopicCMD)
	RootCMD.AddCommand(TopicsCMD)

	TopicCMD.AddCommand(DescribeCMD)
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
	Use:   "describe",
	Short: "Describe topic info",
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		topicInfo := k.DescribeTopic(cmd.Context(), args[0])

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
	if !noHeaderFlag {
		fmt.Fprintf(w, "Consumers:\n")
		fmt.Fprintf(w, "\tName\tMembers\n")
		fmt.Fprintf(w, "\t----\t-------\n")
	}
	for _, c := range topicInfo.Consumers {
		fmt.Fprintf(w, "\t%s\t%d\n", c.Name, c.MembersCount)
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

		for _, topic := range k.ListTopics(cmd.Context()) {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.Name, topic.Partitions, topic.Replicas)
		}
		w.Flush()
	},
}
