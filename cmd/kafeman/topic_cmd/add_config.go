package topic_cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func NewAddConfigCmd() *cobra.Command {
	options := newAddConfigOptions()

	cmd := &cobra.Command{
		Use:               "add-config TOPIC KEY VALUE",
		Short:             "Add config key/value pair to topic",
		Example:           "kafeman topic add-config topic_name compression.type gzip",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Args:              cobra.ExactArgs(3),
		Run:               options.run,
	}

	return cmd
}

func newAddConfigOptions() *addConfigOptions {
	return &addConfigOptions{}
}

type addConfigOptions struct {
}

func (a *addConfigOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)

	topic := args[0]
	key := args[1]
	value := args[2]

	err := k.AddConfigRecord(cmd.Context(), kafeman.AddConfigRecordCommand{
		Topic: topic,
		Key:   key,
		Value: value,
	})
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("Added config %v=%v to topic %v.\n", key, value, topic)
}
