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

	"github.com/spf13/cobra"
)

func App() *cobra.Command {
	kafeman := kafeman_cmd.NewKafemanCMD()
	kafeman.AddCommand(config_cmd.NewConfigCMD())

	kafeman.AddCommand(completion_cmd.NewCompletion(kafeman))
	kafeman.AddCommand(consume_cmd.NewConsumeCMD())
	kafeman.AddCommand(group_cmd.NewGroupCMD())
	kafeman.AddCommand(group_cmd.NewGroupsCMD())
	kafeman.AddCommand(produce_cmd.NewProduceCMD())
	kafeman.AddCommand(produce_cmd.NewProduceExampleCMD())
	kafeman.AddCommand(topic_cmd.NewTopicCMD())
	kafeman.AddCommand(topic_cmd.NewTopicsCMD())
	kafeman.AddCommand(replicate_cmd.NewReplicateCMD())

	return kafeman
}
