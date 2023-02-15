package app

import (
	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	config_cmd "github.com/worldbug/kafeman/internal/command/config"
	consume_cmd "github.com/worldbug/kafeman/internal/command/consume"
	group_cmd "github.com/worldbug/kafeman/internal/command/group"
	kafeman_cmd "github.com/worldbug/kafeman/internal/command/kafeman"
	produce_cmd "github.com/worldbug/kafeman/internal/command/produce"
	replicate_cmd "github.com/worldbug/kafeman/internal/command/replicate"
	topic_cmd "github.com/worldbug/kafeman/internal/command/topic"

	"github.com/worldbug/kafeman/internal/config"

	"github.com/spf13/cobra"
)

func App() *cobra.Command {
	config, err := config.LoadConfig("")
	if err != nil {
		command.ExitWithErr("Can`t load config: %+v", err)
	}

	kafeman := kafeman_cmd.NewKafemanCMD(config)

	kafeman.AddCommand(completion_cmd.NewCompletion(kafeman))
	kafeman.AddCommand(config_cmd.NewConfigCMD(kafeman, config))
	kafeman.AddCommand(consume_cmd.NewConsumeCMD(config))
	kafeman.AddCommand(group_cmd.NewGroupCMD())
	kafeman.AddCommand(produce_cmd.NewProduceCMD())
	kafeman.AddCommand(replicate_cmd.NewReplicateCMD(config))
	kafeman.AddCommand(topic_cmd.NewTopicCMD())
	kafeman.AddCommand(topic_cmd.NewTopicsCMD())

	return kafeman
}
