package topic_cmd

import (
	"github.com/spf13/cobra"
)

func NewTopicCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Create and describe topics.",
	}

	cmd.AddCommand(NewDescribeCMD())
	cmd.AddCommand(NewTopicConsumersCMD())
	cmd.AddCommand(NewDeleteTopicCMD())
	cmd.AddCommand(NewLSTopicsCMD())
	cmd.AddCommand(NewCreateTopicCmd())
	cmd.AddCommand(NewAddConfigCmd())
	cmd.AddCommand(NewTopicSetConfigCMD())
	cmd.AddCommand(NewUpdateTopicCmd())

	return cmd
}
