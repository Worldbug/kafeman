package topic_cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/logger"
)

func NewTopicSetConfigCMD() *cobra.Command {
	options := newTopicSetConfigOptions()

	cmd := &cobra.Command{
		Use:               "set-config",
		Short:             "set topic config. requires Kafka >=2.3.0 on broker side and kafeman cluster config.",
		Example:           "kafeman topic set-config topic.name cleanup.policy=delete",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	return cmd
}

func newTopicSetConfigOptions() *topicSetConfigOptions {
	return &topicSetConfigOptions{}
}

type topicSetConfigOptions struct{}

func (t *topicSetConfigOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)

	topic := args[0]

	splt := strings.Split(args[1], ",")
	configs := make(map[string]string)

	for _, kv := range splt {
		s := strings.Split(kv, "=")

		if len(s) != 2 {
			continue
		}

		configs[s[0]] = s[1]
	}

	if len(configs) < 1 {
		logger.Errorf("No valid configs found")
	}

	err := k.SetConfigValueTopic(cmd.Context(), kafeman.SetConfigValueTopicCommand{
		Topic:  topic,
		Values: configs,
	})
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("\xE2\x9C\x85 Updated config.")
}
