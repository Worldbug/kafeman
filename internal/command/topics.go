package command

import (
	"fmt"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/segmentio/kafka-go"
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

var DescribeCMD = &cobra.Command{
	Use:   "describe",
	Short: "Describe topic info",
	Run: func(cmd *cobra.Command, args []string) {
		cli := kafka.Client{
			Addr: kafka.TCP(conf.GetCurrentCluster().Brokers...),
		}
		topic := args[0]
		resp, err := cli.ListOffsets(cmd.Context(), &kafka.ListOffsetsRequest{
			Topics: map[string][]kafka.OffsetRequest{
				topic: {
					{Partition: 0, Timestamp: time.Now().Unix()},
				},
			},
		})

		fmt.Fprintln(outWriter, resp, err)

	},
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
		//addrs := strings.Join(conf.GetCurrentCluster().Brokers, ",")
		conn, err := kafka.Dial("tcp", conf.GetCurrentCluster().Brokers[0])
		if err != nil {
			panic(err.Error())
		}
		defer conn.Close()

		partitions, err := conn.ReadPartitions()
		if err != nil {
			panic(err.Error())
		}

		m := map[string]struct {
			partitions int32
			replicas   int32
		}{}

		for _, p := range partitions {
			if info, ok := m[p.Topic]; ok {
				info.partitions++
				info.replicas = int32(len(p.Replicas))
				m[p.Topic] = info
				continue
			}

			m[p.Topic] = struct {
				partitions int32
				replicas   int32
			}{
				1, int32(len(p.Replicas)),
			}
		}

		sortedTopics := make(
			[]struct {
				name       string
				partitions int32
				replicas   int32
			}, len(m))

		i := 0
		for name, topic := range m {
			sortedTopics[i].name = name
			sortedTopics[i].partitions = topic.partitions
			sortedTopics[i].replicas = topic.replicas
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
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", topic.name, topic.partitions, topic.replicas)
		}
		w.Flush()
	},
}
