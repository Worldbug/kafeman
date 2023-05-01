package group_cmd

import (
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
)

func NewGroupDescribeCMD() *cobra.Command {
	options := newGroupDescribeOptions()

	cmd := &cobra.Command{
		Use:               "describe",
		Short:             "Describe consumer group",
		Example:           "kafeman group describe group_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewGroupCompletion(),
		Run:               options.run,
	}

	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")
	cmd.Flags().BoolVar(&options.printAll, "full", false, "Print completed info")
	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")

	return cmd
}

func newGroupDescribeOptions() *groupDescribeOptions {
	return &groupDescribeOptions{
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
		out:              os.Stdout,
	}
}

type groupDescribeOptions struct {
	asJson   bool
	printAll bool

	out io.Writer

	common.PrettyPrintFlags
}

func (g *groupDescribeOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	group := k.DescribeGroup(cmd.Context(), args[0])

	if g.asJson {
		common.PrintJson(group)
		return
	}

	g.groupDescribePrint(group)
}

func (g *groupDescribeOptions) groupDescribePrint(group models.Group) {
	w := tabwriter.NewWriter(g.out, g.MinWidth, g.Width, g.Padding, g.PadChar, g.Flags)
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

	if !g.printAll {
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
