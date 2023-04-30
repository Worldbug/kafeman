package produce_cmd

import (
	"fmt"
	"os"

	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/command/global_config"

	"github.com/worldbug/kafeman/internal/command"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/spf13/cobra"
)

func NewProduceExampleCMD() *cobra.Command {
	options := newProduceOptions(global_config.Config)

	cmd := &cobra.Command{
		Use:               "example",
		Short:             "Print example message scheme in topic (if config has proto scheme model) BETA",
		Example:           "kafeman example topic_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		PreRun:            options.setupProtoDescriptorRegistry,
		Run: func(cmd *cobra.Command, args []string) {
			topic, _ := global_config.GetTopicByName(args[0])
			// TODO: add other encoders support
			decoder, err := serializers.NewProtobufSerializer(topic.ProtoPaths, topic.ProtoType)
			if err != nil {
				command.ExitWithErr("%+v", err)
			}
			example := decoder.GetExample(topic.ProtoType)
			// TODO: сделать заполнение семпла базовыми данными
			fmt.Fprintf(os.Stdout, "%+v", example)
		},
	}

	return cmd
}

func newProduceOptions(config *config.Configuration) *produceOptions {
	return &produceOptions{
		config: config,
	}
}

type produceOptions struct {
	config *config.Configuration

	key         string
	partitioner string
	partition   int32
	timestamp   string
	bufferSize  int

	encoding string

	// TODO: refactor
	protoFiles    []string
	protoExclude  []string
	protoType     string
	protoRegistry *serializers.DescriptorRegistry
}

func (p *produceOptions) run(cmd *cobra.Command, args []string) {
	k := kafeman.Newkafeman(p.config)

	produceCommand := kafeman.ProduceCommand{
		Topic:       args[0],
		BufferSize:  p.bufferSize,
		Input:       os.Stdin,
		Output:      os.Stdout,
		Partition:   p.partition,
		Partitioner: p.partitioner,
	}

	encoder, err := p.getEncoder(produceCommand)
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	k.Produce(cmd.Context(), produceCommand, encoder)
}

func (p *produceOptions) getEncoder(cmd kafeman.ProduceCommand) (kafeman.Encoder, error) {
	topicConfig, ok := p.config.GetTopicByName(cmd.Topic)
	if !ok && p.encoding == "" {
		return serializers.NewRawSerializer(), nil
	}

	// override encoding
	if p.encoding != "" {
		topicConfig.Encoding = config.Encoding(p.encoding)
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

func (p *produceOptions) setupProtoDescriptorRegistry(cmd *cobra.Command, args []string) {
	if p.protoType != "" {
		r, err := serializers.NewDescriptorRegistry(p.protoFiles, p.protoExclude)
		if err != nil {
			command.ExitWithErr("Failed to load protobuf files: %v\n", err)
		}

		p.protoRegistry = r
	}
}

func NewProduceCMD() *cobra.Command {
	options := newProduceOptions(global_config.Config)

	cmd := &cobra.Command{
		Use:               "produce",
		Short:             "Produce record. Reads data from stdin.",
		Example:           "kafeman produce topic_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		PreRun:            options.setupProtoDescriptorRegistry,
		Run:               options.run,
	}

	cmd.Flags().StringVarP(&options.key, "key", "k", "", "Key for the record. Currently only strings are supported.")
	cmd.Flags().StringVar(&options.encoding, "force-encoding", "", "Fore set encoding one of [raw,proto,avro,msgpack,base64]")
	cmd.RegisterFlagCompletionFunc("force-encoding", completion_cmd.NewEncodingCompletion())
	cmd.Flags().StringVar(&options.partitioner, "partitioner", "", "Select partitioner: [jvm|rand|rr|hash]")
	cmd.RegisterFlagCompletionFunc("partitioner", completion_cmd.NewPartitionerCompletion())
	cmd.Flags().StringVar(&options.timestamp, "timestamp", "", "Select timestamp for record")
	cmd.Flags().Int32VarP(&options.partition, "partition", "p", -1, "Partition to produce to")
	cmd.Flags().IntVarP(&options.bufferSize, "line-length-limit", "", 0, "line length limit in line input mode")

	return cmd
}
