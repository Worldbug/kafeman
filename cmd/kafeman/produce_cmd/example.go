package produce_cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/serializers"
)

func NewProduceExampleCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "example",
		Short:             "Print example message scheme in topic (if config has proto scheme model) BETA",
		Example:           "kafeman example topic_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run: func(cmd *cobra.Command, args []string) {
			topic, _ := run_configuration.GetTopicByName(args[0])
			// TODO: add other encoders support
			decoder, err := serializers.NewProtobufSerializer(topic.ProtoPaths, topic.ProtoExcludePaths, topic.ProtoType)
			if err != nil {
				common.ExitWithErr("%+v", err)
			}
			example := decoder.GetExample(topic.ProtoType)
			// TODO: сделать заполнение семпла базовыми данными
			fmt.Fprintf(os.Stdout, "%+v", example)
		},
	}

	return cmd
}
