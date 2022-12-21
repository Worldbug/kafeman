package command

import (
	"fmt"
	"os"

	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/spf13/cobra"
)

var (
	keyFlag         string
	partitionerFlag string
	partitionFlag   int32
	timestampFlag   string
	bufferSizeFlag  int
)

func init() {
	RootCMD.AddCommand(ProduceCMD)
	RootCMD.AddCommand(ProduceExample)

	ProduceCMD.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	ProduceCMD.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	ProduceCMD.Flags().StringVar(&timestampFlag, "timestamp", "", "Select timestamp for record")
	ProduceCMD.Flags().Int32VarP(&partitionFlag, "partition", "p", -1, "Partition to produce to")
	ProduceCMD.Flags().IntVarP(&bufferSizeFlag, "line-length-limit", "", 0, "line length limit in line input mode")
}

var ProduceExample = &cobra.Command{
	Use:               "example TOPIC",
	Short:             "Print example message scheme in topic (if config has proto scheme model) BETA",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		topic := conf.Topics[args[0]]
		// TODO: add other encoders support
		decoder, err := serializers.NewProtobufSerializer(topic.ProtoPaths, topic.ProtoType)
		if err != nil {
			errorExit("%+v", err)
		}
		example := decoder.GetExample(topic.ProtoType)
		// TODO: сделать заполнение семпла базовыми данными
		fmt.Fprintf(os.Stdout, "%+v", example)
	},
}

var ProduceCMD = &cobra.Command{
	Use:               "produce TOPIC",
	Short:             "Produce record. Reads data from stdin.",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		k := kafeman.Newkafeman(conf)

		command := kafeman.ProduceCMD{
			Topic:      args[0],
			BufferSize: bufferSizeFlag,
			Input:      os.Stdin,
			Output:     os.Stdout,
		}

		k.Produce(cmd.Context(), command)
	},
}
