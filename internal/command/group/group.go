package group_cmd

import (
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"

	"github.com/spf13/cobra"
)

func NewGroupCMD(config *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Display information about consumer groups.",
	}

	cmd.AddCommand(NewGroupLSCMD(config))
	cmd.AddCommand(NewGroupDescribeCMD(config))
	cmd.AddCommand(NewGroupDeleteCMD(config))
	cmd.AddCommand(NewGroupCommitCMD(config))

	return cmd
}

func NewGroupsCMD(config *config.Configuration) *cobra.Command {
	groups := NewGroupLSCMD(config)

	cmd := &cobra.Command{
		Use:   "groups",
		Short: "List groups",
		Run:   groups.Run,
	}

	return cmd
}

func newGroupDeleteOptions(config *config.Configuration) *groupDeleteOptions {
	return &groupDeleteOptions{}
}

type groupDeleteOptions struct {
	config *config.Configuration
}

func (g *groupDeleteOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(g.config)

	var group string
	if len(args) == 1 {
		group = args[0]
	}

	err := k.DeleteGroup(group)
	if err != nil {
		command.ExitWithErr("Could not delete consumer group %v: %v", group, err.Error())
	}

	fmt.Fprintf(os.Stdout, "Deleted consumer group %v.\n", group)
}

func NewGroupDeleteCMD(config *config.Configuration) *cobra.Command {
	options := newGroupDeleteOptions(config)

	cmd := &cobra.Command{
		Use:               "delete",
		Short:             "Delete group",
		Example:           "kafeman group delete group_name",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completion_cmd.NewGroupCompletion(config),
		Run:               options.run,
	}

	return cmd
}

func newGroupLsOptions(config *config.Configuration) *groupLSOptions {
	return &groupLSOptions{
		config:           config,
		out:              os.Stdout,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
	}
}

type groupLSOptions struct {
	config *config.Configuration
	asJson bool

	out io.Writer

	command.PrettyPrintFlags
}

func (g *groupLSOptions) run(cmd *cobra.Command, args []string) {
	// TODO: надо тут поправить
	k := kafeman.Newkafeman(g.config)
	groupList, err := k.GetGroupsList(cmd.Context())
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	groupDescs, err := k.DescribeGroups(cmd.Context(), groupList)
	if err != nil {
		command.ExitWithErr("Unable to describe consumer groups: %v\n", err)
	}

	if g.asJson {
		command.PrintJson(groupDescs)
		return
	}

	g.groupListPrint(groupDescs)
}

func (g *groupLSOptions) groupListPrint(groupDescs []kafeman.GroupInfo) {
	w := tabwriter.NewWriter(g.out, g.MinWidth, g.Width, g.Padding, g.PadChar, g.Flags)

	if !g.NoHeader {
		fmt.Fprintf(w, "NAME\tSTATE\tCONSUMERS\t\n")
	}

	for _, detail := range groupDescs {
		fmt.Fprintf(w, "%v\t%v\t%v\t\n", detail.Name, detail.State, detail.Consumers)
	}

	w.Flush()
}

func NewGroupLSCMD(config *config.Configuration) *cobra.Command {
	options := newGroupLsOptions(config)

	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List groups",
		Args:  cobra.NoArgs,
		Run:   options.run,
	}

	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")
	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")

	return cmd
}

func newGroupDescribeOptions(config *config.Configuration) *groupDescribeOptions {
	return &groupDescribeOptions{
		config:           config,
		PrettyPrintFlags: command.NewPrettyPrintFlags(),
		out:              os.Stdout,
	}
}

type groupDescribeOptions struct {
	config *config.Configuration

	asJson   bool
	printAll bool

	out io.Writer

	command.PrettyPrintFlags
}

func (g *groupDescribeOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(g.config)
	group := k.DescribeGroup(cmd.Context(), args[0])

	if g.asJson {
		command.PrintJson(group)
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

func NewGroupDescribeCMD(config *config.Configuration) *cobra.Command {
	options := newGroupDescribeOptions(config)

	cmd := &cobra.Command{
		Use:               "describe",
		Short:             "Describe consumer group",
		Example:           "kafeman group describe group_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewGroupCompletion(config),
		Run:               options.run,
	}

	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")
	cmd.Flags().BoolVar(&options.printAll, "full", false, "Print completed info")
	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")

	return cmd
}

func newGroupCommitOptions(config *config.Configuration) *groupCommitOptions {
	return &groupCommitOptions{
		config: config,
	}
}

type groupCommitOptions struct {
	config *config.Configuration

	fromJson      bool
	allPartitions bool
	noConfirm     bool
	topic         string
	offset        string
	partition     int32
}

func (g *groupCommitOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(g.config)
	group := args[0]
	offsets := make([]models.Offset, 0)
	// partitions := make([]int, 0)

	// if fromJsonFlag {
	// TODO: commit from json
	//}

	if g.allPartitions {
		t, err := k.GetTopicInfo(cmd.Context(), g.topic)
		if err != nil {
			command.ExitWithErr("%+v", err)
		}

		o := command.GetOffsetFromFlag(g.offset)
		for i := t.Partitions - 1; i >= 0; i-- {
			offsets = append(offsets, models.Offset{
				Partition: int32(i),
				Offset:    o,
			})
			// partitions = append(partitions, i)
		}
	}

	// TODO:
	// if !noConfirmFlag {
	//
	// }

	k.SetGroupOffset(cmd.Context(), group, g.topic, offsets)
}

func NewGroupCommitCMD(config *config.Configuration) *cobra.Command {
	options := newGroupCommitOptions(config)

	cmd := &cobra.Command{
		Use:     "commit",
		Short:   "Set offset for given consumer group",
		Long:    "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Example: "kafeman group commit group_name -t topic_name --all-partitions  --offset 100500",
		Args:    cobra.ExactArgs(1),
		Run:     options.run,
	}

	cmd.Flags().BoolVar(&options.fromJson, "json", false, "Parse json from std and set values")
	cmd.Flags().BoolVar(&options.allPartitions, "all-partitions", false, "apply to all partitions")
	cmd.Flags().Int32Var(&options.partition, "p", 0, "partition")
	cmd.Flags().StringVar(&options.offset, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	cmd.RegisterFlagCompletionFunc("offset", completion_cmd.NewOffsetCompletion())
	cmd.Flags().StringVarP(&options.topic, "topic", "t", "", "topic to set offset")
	cmd.RegisterFlagCompletionFunc("topic", completion_cmd.NewTopicCompletion(config))
	cmd.Flags().BoolVar(&options.noConfirm, "y", false, "Do not prompt for confirmation")

	return cmd
}
