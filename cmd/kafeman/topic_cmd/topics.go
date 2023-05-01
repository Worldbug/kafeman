package topic_cmd

import "github.com/spf13/cobra"

func NewTopicsCMD() *cobra.Command {
	cmd := NewLSTopicsCMD()
	cmd.Use = "topics"

	return cmd
}
