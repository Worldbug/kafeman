package command

import (
	"fmt"
	"os"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/spf13/cobra"
)

var (
	keyFlag         string
	partitionerFlag string // TODO: implement
	partitionFlag   int32
	timestampFlag   string
	bufferSizeFlag  int
)

func init() {
	RootCMD.AddCommand(ProduceCMD)
	RootCMD.AddCommand(ProduceExample)

	ProduceCMD.Flags().StringVarP(&keyFlag, "key", "k", "", "Key for the record. Currently only strings are supported.")
	ProduceCMD.Flags().StringVar(&encoding, "force-encoding", "", "Fore set encoding one of [raw,proto,avro,msgpack,base64]")
	ProduceCMD.RegisterFlagCompletionFunc("force-encoding", encodingCompletion)
	ProduceCMD.Flags().StringVar(&partitionerFlag, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	ProduceCMD.RegisterFlagCompletionFunc("partitioner", partitionerCompletion)
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

		command := kafeman.ProduceCommand{
			Topic:      args[0],
			BufferSize: bufferSizeFlag,
			Input:      os.Stdin,
			Output:     os.Stdout,
		}

		encoder, err := getEncoder(command)
		if err != nil {
			errorExit("%+v", err)
		}

		k.Produce(cmd.Context(), command, encoder)
	},
}

func getEncoder(cmd kafeman.ProduceCommand) (kafeman.Encoder, error) {
	topicConfig, ok := conf.Topics[cmd.Topic]
	if !ok && encoding == "" {
		return serializers.NewRawSerializer(), nil
	}

	// override encoding
	if encoding != "" {
		topicConfig.Encoding = config.Encoding(encoding)
	}

	// force type
	switch topicConfig.Encoding {
	case config.RAW:
		return serializers.NewRawSerializer(), nil
	case config.Avro:
		return serializers.NewAvroSerializer(topicConfig.AvroSchemaURL, topicConfig.AvroSchemaID)
	case config.Protobuf:
		return serializers.NewProtobufSerializer(topicConfig.ProtoPaths, topicConfig.ProtoType)
	case config.MSGPack:
		return serializers.NewMessagePackSerializer(), nil
	case config.Base64:
		return serializers.NewBase64Serializer(), nil
	}

	// AVRO DECODER
	if topicConfig.AvroSchemaURL != "" {
		return serializers.NewAvroSerializer(topicConfig.AvroSchemaURL, topicConfig.AvroSchemaID)
	}

	// PROTO DECODER
	if topicConfig.ProtoType != "" || len(topicConfig.ProtoPaths) != 0 {
		return serializers.NewProtobufSerializer(topicConfig.ProtoPaths, topicConfig.ProtoType)
	}

	// RAW DECODER
	return serializers.NewRawSerializer(), nil
}
