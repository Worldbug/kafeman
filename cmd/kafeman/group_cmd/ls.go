package group_cmd

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func NewGroupLSCMD() *cobra.Command {
	options := newGroupLsOptions()

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

func newGroupLsOptions() *groupLSOptions {
	return &groupLSOptions{
		out:              os.Stdout,
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
	}
}

type groupLSOptions struct {
	asJson bool

	out io.Writer

	common.PrettyPrintFlags
}

func (g *groupLSOptions) run(cmd *cobra.Command, args []string) {
	// TODO: надо тут поправить
	k := kafeman.Newkafeman(run_configuration.Config)
	groupList, err := k.GetGroupsList(cmd.Context())
	if err != nil {
		common.ExitWithErr("%+v", err)
	}

	groupDescs, err := k.DescribeGroups(cmd.Context(), groupList)
	if err != nil {
		common.ExitWithErr("Unable to describe consumer groups: %v\n", err)
	}

	if g.asJson {
		common.PrintJson(groupDescs)
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
