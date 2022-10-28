package command

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/Shopify/sarama"
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
		admin := getClusterAdmin()

		topics, err := admin.ListTopics()
		if err != nil {
			errorExit("Unable to list topics: %v\n", err)
		}

		sortedTopics := make(
			[]struct {
				name string
				sarama.TopicDetail
			}, len(topics))

		i := 0
		for name, topic := range topics {
			sortedTopics[i].name = name
			sortedTopics[i].TopicDetail = topic
			i++
		}

		sort.Slice(sortedTopics, func(i int, j int) bool {
			return sortedTopics[i].name < sortedTopics[j].name
		})

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		if !noHeaderFlag {
			fmt.Fprintf(w, "NAME\tPARTITIONS\tREPLICAS\t\n")
		}

		for _, topic := range sortedTopics {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.name, topic.NumPartitions, topic.ReplicationFactor)
		}
		w.Flush()
	},
}
