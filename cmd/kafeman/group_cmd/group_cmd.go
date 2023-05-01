package group_cmd

import (
	"github.com/spf13/cobra"
)

func NewGroupCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "group",
		Short: "Display information about consumer groups.",
	}

	cmd.AddCommand(NewGroupLSCMD())
	cmd.AddCommand(NewGroupDescribeCMD())
	cmd.AddCommand(NewGroupDeleteCMD())
	cmd.AddCommand(NewGroupCommitCMD())

	return cmd
}
