package command

import (
	"fmt"
	"kafeman/internal/kafeman"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func init() {
	RootCMD.AddCommand(groupCmd)
	groupCmd.AddCommand(groupsCmd)
	groupCmd.AddCommand(groupLsCmd)
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
