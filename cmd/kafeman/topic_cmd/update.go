package topic_cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
)

func NewUpdateTopicCmd() *cobra.Command {
	options := newUpdateTopicOptions()

	cmd := &cobra.Command{
		Use:               "update",
		Short:             "Update topic",
		Example:           "kafeman topic update topic_name -p 5 --partition-assignments '[[1,2,3],[1,2,3]]'",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().Int32VarP(&options.partitions, "partitions", "p", int32(1), "Number of partitions")
	cmd.Flags().BoolVar(&options.compact, "compact", false, "Enable topic compaction")
	cmd.Flags().StringVar(&options.partitionAssignments, "partition-assignments", "", "Partition Assignments. Optional. If set in combination with -p, an assignment must be provided for each new partition. Example: '[[1,2,3],[1,2,3]]' (JSON Array syntax) assigns two new partitions to brokers 1,2,3. If used by itself, a reassignment must be provided for all partitions.")
	return cmd
}

func newUpdateTopicOptions() *updateTopicOptions {
	return &updateTopicOptions{}
}

type updateTopicOptions struct {
	partitionAssignments string
	// TODO:
	compact    bool
	partitions int32
}

func (u *updateTopicOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	topic := args[0]

	if u.partitions == -1 && u.partitionAssignments == "" {
		common.ExitWithErr("Number of partitions and/or partition assignments must be given")
	}

	var assignments [][]int32
	if u.partitionAssignments != "" {
		if err := json.Unmarshal([]byte(u.partitionAssignments), &assignments); err != nil {
			common.ExitWithErr("Invalid partition assignments: %v", err)
		}
	}

	err := k.UpdateTopic(cmd.Context(), kafeman.UpdateTopicCommand{
		Topic:           topic,
		PartitionsCount: u.partitions,
		Assignments:     assignments,
	})
	if err != nil {
		os.Exit(1)
	}

	fmt.Printf("\xE2\x9C\x85 Updated topic!\n")
}
