package group_cmd

import (
	"fmt"

	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
)

func NewGroupCommitCMD() *cobra.Command {
	options := newGroupCommitOptions()

	cmd := &cobra.Command{
		Use:               "commit",
		Short:             "Set offset for given consumer group",
		Long:              "Set offset for a given consumer group, creates one if it does not exist. Offsets cannot be set on a consumer group with active consumers.",
		Example:           "kafeman group commit group_name -t topic_name --all-partitions  --offset 100500",
		Args:              cobra.ExactArgs(1),
		Run:               options.run,
		ValidArgsFunction: completion_cmd.NewGroupCompletion(),
	}

	cmd.Flags().BoolVar(&options.fromJson, "json", false, "Parse json from std and set values")
	cmd.Flags().Int32Var(&options.partition, "p", 0, "partition")
	cmd.Flags().StringVar(&options.offset, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	cmd.RegisterFlagCompletionFunc("offset", completion_cmd.NewOffsetCompletion())
	cmd.Flags().StringVarP(&options.topic, "topic", "t", "", "topic to set offset")
	cmd.RegisterFlagCompletionFunc("topic", completion_cmd.NewTopicCompletion())
	cmd.Flags().BoolVarP(&options.noConfirm, "yes", "y", false, "Do not prompt for confirmation")
	cmd.Flags().StringVar(&options.time, "time", "", "time to set offset in UTC (format 2023-05-09T00:00:00)")
	cmd.RegisterFlagCompletionFunc("time", completion_cmd.NewTimeCompletion())
	cmd.Flags().StringArrayVar(&options.offsets, "set", []string{}, "offsets to set for group on partitions")

	return cmd
}

func newGroupCommitOptions() *groupCommitOptions {
	return &groupCommitOptions{}
}

type groupCommitOptions struct {
	fromJson  bool
	noConfirm bool
	topic     string
	offset    string
	partition int32
	time      string
	offsets   []string
}

func (g *groupCommitOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	group := args[0]

	offsets, err := parseOffsets(g.offsets)
	if err != nil {
		common.ExitWithErr("%+v", err)
	}

	if g.time != "" {
		timeOffset := common.ParseTime(g.time)
		rawOffsets, err := admin.NewAdmin(run_configuration.Config).
			GetOffsetsByTime(cmd.Context(), g.topic, timeOffset)
		if err != nil {
			common.ExitWithErr("%+v\n", err)
		}

		// TODO: вытащить все лишнее из models.Offset
		for i, offset := range rawOffsets {
			offsets = append(offsets, models.Offset{
				Partition: int32(i),
				Offset:    offset,
			})
		}
	}

	if len(g.offsets) == 0 {
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
		}
	}

	if !g.noConfirm {
		prompt := promptui.Prompt{
			Label:     "Reset offsets as described",
			IsConfirm: true,
		}

		_, err = prompt.Run()
		if err != nil {
			common.ExitWithErr("Aborted, exiting.\n")
			return
		}
	}

	err = k.SetGroupOffset(cmd.Context(), group, g.topic, offsets)
	if err != nil {
		common.ExitWithErr("%+v: ", err)
	}
}

// TODO: протестировать уникальность офсетов
func parseOffsets(rawOffsets []string) ([]models.Offset, error) {
	offsets := make([]models.Offset, 0, len(rawOffsets))

	for _, rawOffset := range rawOffsets {
		offset := models.Offset{}
		_, err := fmt.Sscanf(rawOffset, "%d=%d", &offset.Partition, &offset.Offset)
		if err != nil {
			return nil, err
		}

		offsets = append(offsets, offset)
	}

	return offsets, nil
}
