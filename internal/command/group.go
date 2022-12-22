package command

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"

	"github.com/spf13/cobra"
)

var (
	asJsonFlag        bool
	fromJsonFlag      bool
	printAllFlag      bool
	allPartitionsFlag bool
	noConfirmFlag     bool
	partitionFag      int
	topicFlag         string
)

func init() {
	RootCMD.AddCommand(GroupCMD)
	RootCMD.AddCommand(GroupsCMD)

	GroupLsCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")
	GroupsCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")

	GroupCMD.AddCommand(GroupsCMD)
	GroupCMD.AddCommand(GroupLsCMD)
	GroupCMD.AddCommand(GroupDescribeCMD)
	GroupCMD.AddCommand(GroupDeleteCMD)
	GroupCMD.AddCommand(GroupCommitCMD)

	GroupDescribeCMD.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")
	GroupDescribeCMD.Flags().BoolVar(&printAllFlag, "full", false, "Print completed info")
	GroupCommitCMD.Flags().BoolVar(&fromJsonFlag, "json", false, "Parse json from std and set values")
	GroupCommitCMD.Flags().BoolVar(&allPartitionsFlag, "all-partitions", false, "apply to all partitions")
	GroupCommitCMD.Flags().IntVar(&partitionFag, "p", 0, "partition")
	GroupCommitCMD.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	GroupCommitCMD.RegisterFlagCompletionFunc("offset", offsetCompletion)
	GroupCommitCMD.Flags().StringVarP(&topicFlag, "topic", "t", "", "topic to set offset")
	GroupCommitCMD.RegisterFlagCompletionFunc("topic", topicCompletion)
	GroupCommitCMD.Flags().BoolVar(&noConfirmFlag, "y", false, "Do not prompt for confirmation")
}

var GroupCMD = &cobra.Command{
	Use:   "group",
	Short: "Display information about consumer groups.",
}

var GroupsCMD = &cobra.Command{
	Use:   "groups",
	Short: "List groups",
	Run:   GroupLsCMD.Run,
}

var GroupDeleteCMD = &cobra.Command{
	Use:               "delete",
	Short:             "Delete group",
	Args:              cobra.MaximumNArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)

		var group string
		if len(args) == 1 {
			group = args[0]
		}

		err := k.DeleteGroup(group)
		if err != nil {
			errorExit("Could not delete consumer group %v: %v", group, err.Error())
		}

		fmt.Fprintf(os.Stdout, "Deleted consumer group %v.\n", group)
	},
}

var GroupLsCMD = &cobra.Command{
	Use:   "ls",
	Short: "List groups",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: надо тут поправить
		k := kafeman.Newkafeman(conf)
		groupList, err := k.GetGroupsList(cmd.Context())
		if err != nil {
			errorExit("%+v", err)
		}

		groupDescs, err := k.DescribeGroups(cmd.Context(), groupList)
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		if asJsonFlag {
			printJson(groupDescs)
			return
		}

		groupListPrint(groupDescs)

	},
}

func groupListPrint(groupDescs []kafeman.GroupInfo) {
	w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

	if !noHeaderFlag {
		fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
	}

	for _, detail := range groupDescs {
		fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.Name, detail.State, detail.Consumers)
	}

	w.Flush()
}

var GroupDescribeCMD = &cobra.Command{
	Use:               "describe",
	Short:             "Describe consumer group",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		group := k.DescribeGroup(cmd.Context(), args[0])

		if asJsonFlag {
			jsonGroupDescribe(group)
			return
		}

		groupDescribePrint(group)

	}}

// TODO: переписать
var GroupCommitCMD = &cobra.Command{
	Use:   "commit",
	Short: "Set offset for given consumer group",
	Long:  "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)
		group := args[0]
		offsets := make([]models.Offset, 0)
		partitions := make([]int, 0)

		if fromJsonFlag {
			// TODO: commit from json
		}

		if allPartitionsFlag {
			t, err := k.GetTopicInfo(cmd.Context(), topicFlag)
			if err != nil {
				errorExit("%+v", err)
			}

			o := getOffsetFromFlag()

			for i := t.Partitions - 1; i >= 0; i-- {
				offsets = append(offsets, models.Offset{
					Partition: int32(i),
					Offset:    o,
				})
				partitions = append(partitions, i)
			}
		}

		if !noConfirmFlag {

		}

		k.SetGroupOffset(cmd.Context(), group, topicFlag, offsets)
	},
}

func jsonGroupDescribe(group models.Group) {
	var output []byte
	if printAllFlag {
		output, _ = json.Marshal(group)
		fmt.Fprintln(outWriter, string(output))
		return
	}

	output, _ = json.Marshal(group.Offsets)
	fmt.Fprintln(outWriter, string(output))
}

func groupDescribePrint(group models.Group) {
	w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
	defer w.Flush()

	fmt.Fprintf(w, "Group ID:\t%v\nState:\t%v\n", group.GroupID, group.State)

	for topic, offsets := range group.Offsets {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, topic)
		fmt.Fprintln(w, "\tPartition\tOffset\tHigh Watermark\tLag")
		fmt.Fprintln(w, "\t---------\t------\t--------------\t---")

		sort.Slice(offsets, func(i, j int) bool {
			return offsets[i].Partition < offsets[j].Partition
		})

		for _, o := range offsets {
			fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\n", o.Partition, o.Offset, o.HightWatermark, o.Lag)
		}
	}

	if !printAllFlag {
		return
	}

	for _, m := range group.Members {
		fmt.Fprintf(w, "Member:\t%v\nHost:\t%v\n", m.ID, m.Host)
		fmt.Fprintf(w, "\tTopic\tPartitions\n")
		fmt.Fprintf(w, "\t-----\t----------\n")
		for _, a := range m.Assignments {
			fmt.Fprintf(w, "\t%v\t%v\n", a.Topic, a.Partitions)
		}
		fmt.Fprintf(w, "\n")
	}

}

func validGroupArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	k := kafeman.Newkafeman(conf)
	groupList, err := k.GetGroupsList(cmd.Context())
	if err != nil {
		fmt.Fprintln(errWriter, err)
	}

	return groupList, cobra.ShellCompDirectiveNoFileComp
}
