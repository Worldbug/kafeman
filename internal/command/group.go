package command

import (
	"encoding/json"
	"fmt"
	"kafeman/internal/kafeman"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

var (
	asJsonFlag bool
)

func init() {
	RootCMD.AddCommand(groupCmd)
	groupCmd.AddCommand(groupsCmd)
	groupCmd.AddCommand(groupLsCmd)
	groupCmd.AddCommand(groupDescribeCmd)

	groupDescribeCmd.Flags().BoolVar(&asJsonFlag, "json", false, "Print data as json")
}

var groupCmd = &cobra.Command{
	Use:   "group",
	Short: "Display information about consumer groups.",
}

var groupsCmd = &cobra.Command{
	Use:   "groups",
	Short: "List groups",
	Run:   groupLsCmd.Run,
}

// var groupDeleteCmd = &cobra.Command{
// 	Use:               "delete",
// 	Short:             "Delete group",
// 	Args:              cobra.MaximumNArgs(1),
// 	ValidArgsFunction: validGroupArgs,
// 	Run: func(cmd *cobra.Command, args []string) {
// 		admin := getClusterAdmin()
// 		var group string
// 		if len(args) == 1 {
// 			group = args[0]
// 		}
// 		err := admin.DeleteConsumerGroup(group)
// 		if err != nil {
// 			errorExit("Could not delete consumer group %v: %v\n", group, err.Error())
// 		} else {
// 			fmt.Printf("Deleted consumer group %v.\n", group)
// 		}
//
// 	},
// }

var groupLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List groups",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf, nil, nil)
		groupList, err := k.GetGroupsList(cmd.Context())
		if err != nil {
			fmt.Println(err)
		}

		sort.Slice(groupList, func(i int, j int) bool {
			return groupList[i] < groupList[j]
		})

		w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		if !noHeaderFlag {
			fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
		}

		groupDescs, err := k.DescribeGroups(cmd.Context(), groupList)
		if err != nil {
			errorExit("Unable to describe consumer groups: %v\n", err)
		}

		for _, detail := range groupDescs {
			fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.Name, detail.State, detail.Consumers)
		}

		w.Flush()
	},
}

var groupDescribeCmd = &cobra.Command{
	Use:               "describe",
	Short:             "Describe consumer group",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validGroupArgs,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf, nil, nil)
		group := k.DescribeGroup(cmd.Context(), args[0])

		if asJsonFlag {
			jsonGroupDescribe(group)
			return
		}

		textGroupDescribe(group)

	}}

func jsonGroupDescribe(group kafeman.Group) {
	output, _ := json.Marshal(group)
	fmt.Fprintln(outWriter, string(output))

}

func textGroupDescribe(group kafeman.Group) {
	w := tabwriter.NewWriter(outWriter, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
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

	// for _, m := range group.Members {
	// 	fmt.Fprintf(w, "Member:\t%v\nHost:\t%v\n", m.ID, m.Host)
	// 	fmt.Fprintf(w, "\tTopic\tPartitions\n")
	// 	fmt.Fprintf(w, "\t-----\t----------\n")
	// 	for _, a := range m.Assignments {
	// 		fmt.Fprintf(w, "\t%v\t%v\n", a.Topic, a.Partitions)
	// 	}
	// 	fmt.Fprintf(w, "\n")
	// }

	w.Flush()
}

func validGroupArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	k := kafeman.Newkafeman(conf, nil, nil)
	groupList, err := k.GetGroupsList(cmd.Context())
	if err != nil {
		fmt.Fprintln(errWriter, err)
	}

	return groupList, cobra.ShellCompDirectiveNoFileComp
}
