package main

import (
	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/config_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/consume_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/group_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/kafeman_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/produce_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/replicate_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/topic_cmd"
)

func main() {
	if err := app().Execute(); err != nil {
		common.ExitWithErr("Cant launch: %+v", err)
	}
}

func app() *cobra.Command {
	kafeman := kafeman_cmd.NewKafemanCMD()

	kafeman.AddCommand(completion_cmd.NewCompletion(kafeman))
	kafeman.AddCommand(config_cmd.NewConfigCMD())
	kafeman.AddCommand(consume_cmd.NewConsumeCMD())
	kafeman.AddCommand(group_cmd.NewGroupCMD())
	kafeman.AddCommand(alias(group_cmd.NewGroupLSCMD(), "groups"))
	kafeman.AddCommand(produce_cmd.NewProduceCMD())
	kafeman.AddCommand(produce_cmd.NewProduceExampleCMD())
	kafeman.AddCommand(topic_cmd.NewTopicCMD())
	kafeman.AddCommand(alias(topic_cmd.NewLSTopicsCMD(), "topics"))
	kafeman.AddCommand(replicate_cmd.NewReplicateCMD())

	return kafeman
}

func alias(cmd *cobra.Command, alias string) *cobra.Command {
	cmd.Use = alias
	return cmd
}
