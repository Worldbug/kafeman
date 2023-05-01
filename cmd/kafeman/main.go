package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
		fmt.Fprintf(os.Stderr, "err: %+v", err)
		os.Exit(1)
	}
}

func app() *cobra.Command {
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
