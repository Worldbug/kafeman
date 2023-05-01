package group_cmd

import "github.com/spf13/cobra"

func NewGroupsCMD() *cobra.Command {
	groups := NewGroupLSCMD()

	cmd := &cobra.Command{
		Use:   "groups",
		Short: "List groups",
		Run:   groups.Run,
	}

	return cmd
}
