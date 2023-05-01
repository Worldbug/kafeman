package group_cmd

import (
	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
)

func NewGroupCommitCMD() *cobra.Command {
	options := newGroupCommitOptions()

	cmd := &cobra.Command{
		Use:     "commit",
		Short:   "Set offset for given consumer group",
		Long:    "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Example: "kafeman group commit group_name -t topic_name --all-partitions  --offset 100500",
		Args:    cobra.ExactArgs(1),
		Run:     options.run,
	}

	cmd.Flags().BoolVar(&options.fromJson, "json", false, "Parse json from std and set values")
	cmd.Flags().BoolVar(&options.allPartitions, "all-partitions", false, "apply to all partitions")
	cmd.Flags().Int32Var(&options.partition, "p", 0, "partition")
	cmd.Flags().StringVar(&options.offset, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	cmd.RegisterFlagCompletionFunc("offset", completion_cmd.NewOffsetCompletion())
	cmd.Flags().StringVarP(&options.topic, "topic", "t", "", "topic to set offset")
	cmd.RegisterFlagCompletionFunc("topic", completion_cmd.NewTopicCompletion())
	cmd.Flags().BoolVar(&options.noConfirm, "y", false, "Do not prompt for confirmation")

	return cmd
}

func newGroupCommitOptions() *groupCommitOptions {
	return &groupCommitOptions{}
}

type groupCommitOptions struct {
	fromJson      bool
	allPartitions bool
	noConfirm     bool
	topic         string
	offset        string
	partition     int32
}

func (g *groupCommitOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	group := args[0]
	offsets := make([]models.Offset, 0)
	// partitions := make([]int, 0)

	// if fromJsonFlag {
	// TODO: commit from json
	//}

	if g.allPartitions {
		t, err := k.GetTopicInfo(cmd.Context(), g.topic)
		if err != nil {
			common.ExitWithErr("%+v", err)
		}

		o := common.GetOffsetFromFlag(g.offset)
		for i := t.Partitions - 1; i >= 0; i-- {
			offsets = append(offsets, models.Offset{
				Partition: int32(i),
				Offset:    o,
			})
			// partitions = append(partitions, i)
		}
	}

	// TODO:
	// if !noConfirmFlag {
	//
	// }

	k.SetGroupOffset(cmd.Context(), group, g.topic, offsets)
}
