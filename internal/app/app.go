package app

import (
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

// TODO: refactor
const configPath = ""

func App() *cobra.Command {
	config, _ := config.LoadConfig(configPath)

	kafeman := kafeman_cmd.NewKafemanCMD()
	kafeman.AddCommand(config_cmd.NewConfigCMD(kafeman))

	kafeman.AddCommand(completion_cmd.NewCompletion(kafeman))
	kafeman.AddCommand(consume_cmd.NewConsumeCMD(config))
	kafeman.AddCommand(group_cmd.NewGroupCMD(config))
	kafeman.AddCommand(group_cmd.NewGroupsCMD(config))
	kafeman.AddCommand(produce_cmd.NewProduceCMD(config))
	kafeman.AddCommand(produce_cmd.NewProduceExampleCMD(config))
	kafeman.AddCommand(topic_cmd.NewTopicCMD(config))
	kafeman.AddCommand(topic_cmd.NewTopicsCMD(config))
	kafeman.AddCommand(replicate_cmd.NewReplicateCMD(config))

	return kafeman
}
