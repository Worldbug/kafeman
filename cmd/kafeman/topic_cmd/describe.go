package topic_cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
)

func NewDescribeCMD() *cobra.Command {
	options := newDescribeOptions()

	cmd := &cobra.Command{
		Use:               "describe",
		Short:             "Describe topic info",
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().BoolVar(&options.NoHeader, "no-headers", false, "Hide table headers")
	cmd.Flags().BoolVar(&options.asJson, "json", false, "Print data as json")

	return cmd
}

func newDescribeOptions() *describeOptions {
	return &describeOptions{
		out:              os.Stdout,
		PrettyPrintFlags: common.NewPrettyPrintFlags(),
	}
}

type describeOptions struct {
	common.PrettyPrintFlags
	out    io.Writer
	asJson bool
}

func (d *describeOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(run_configuration.Config)
	topicInfo, err := k.DescribeTopic(cmd.Context(), args[0])
	if err != nil {
		common.ExitWithErr("", err)
	}

	if d.asJson {
		d.describeTopicPrintJson(topicInfo)
		return
	}

	d.describeTopicPrint(topicInfo)
}

func (d *describeOptions) describeTopicPrintJson(topicInfo models.TopicInfo) {
	raw, err := json.Marshal(topicInfo)
	if err != nil {
		return
	}

	fmt.Fprintln(d.out, string(raw))
}

func (d *describeOptions) describeTopicPrint(topicInfo models.TopicInfo) {
	w := tabwriter.NewWriter(d.out, d.MinWidth, d.Width, d.Padding, d.PadChar, d.Flags)
	defer w.Flush()

	fmt.Fprintf(w, "Topic:\t%s\n", topicInfo.TopicName)
	w.Flush()

	fmt.Fprintf(w, "Partitions:\n")
	w.Flush()

	if !d.NoHeader {
		fmt.Fprintf(w, "\tPartition\tLog start offset\tLog end offset\tHigh Watermark\tLeader\tReplicas\tISR\t\n")
		fmt.Fprintf(w, "\t---------\t----------------\t--------------\t--------------\t------\t--------\t---\t\n")
	}
	sort.Slice(topicInfo.Partitions, func(i, j int) bool {
		return topicInfo.Partitions[i].Partition < topicInfo.Partitions[j].Partition
	})
	for _, p := range topicInfo.Partitions {
		fmt.Fprintf(w, "\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t\n", p.Partition, p.LogStartOffset, p.LogEndOffset, p.HightWatermark, p.Leader, p.Replicas, p.ISR)
	}
	w.Flush()

	if !d.NoHeader {
		fmt.Fprintf(w, "Config:\n")
		fmt.Fprintf(w, "\tKey\tValue\tRead only\tSensitive\n")
		fmt.Fprintf(w, "\t---\t-----\t---------\t---------\n")
	}

	for _, c := range topicInfo.Config {
		fmt.Fprintf(w, "\t%s\t%s\t%v\t%v\n", c.Name, c.Value, c.ReadOnly, c.Sensitive)
	}

}
