package cluster_cmd

import (
	"github.com/spf13/cobra"
)

func NewClusterCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Kafka cluster actions",
	}

	cmd.AddCommand(NewInfoCMD())

	return cmd
}
