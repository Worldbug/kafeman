package group_cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func NewGroupDeleteCMD() *cobra.Command {
	options := newGroupDeleteOptions()

	cmd := &cobra.Command{
		Use:               "delete",
		Short:             "Delete group",
		Example:           "kafeman group delete group_name",
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completion_cmd.NewGroupCompletion(),
		Run:               options.run,
	}

	return cmd
}

func newGroupDeleteOptions() *groupDeleteOptions {
	return &groupDeleteOptions{}
}

type groupDeleteOptions struct {
}

func (g *groupDeleteOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)

	var group string
	if len(args) == 1 {
		group = args[0]
	}

	err := k.DeleteGroup(group)
	if err != nil {
		common.ExitWithErr("Could not delete consumer group %v: %v", group, err.Error())
	}

	fmt.Fprintf(os.Stdout, "Deleted consumer group %v.\n", group)
}
