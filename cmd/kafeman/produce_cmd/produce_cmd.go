package produce_cmd

import (
	"os"

	"github.com/worldbug/kafeman/cmd/kafeman/common"
	"github.com/worldbug/kafeman/cmd/kafeman/completion_cmd"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/serializers"
	"github.com/worldbug/kafeman/internal/utils"

	"github.com/spf13/cobra"
)

func newProduceOptions() *produceOptions {
	return &produceOptions{}
}

type produceOptions struct {
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
	k := kafeman.Newkafeman(run_configuration.Config)

	produceCommand := kafeman.ProduceCommand{
		Topic:       args[0],
		BufferSize:  p.bufferSize,
		Input:       os.Stdin,
		Output:      os.Stdout,
		Partition:   p.partition,
		Partitioner: p.partitioner,
	}

	topicConfig, _ := run_configuration.GetTopicByName(args[0])
	topicConfig.ProtoType = utils.OrDefault(p.protoType, topicConfig.ProtoType)
	topicConfig.ProtoPaths = utils.OrDefaultSlice(p.protoFiles, topicConfig.ProtoPaths)
	topicConfig.ProtoExcludePaths = utils.OrDefaultSlice(p.protoExclude, topicConfig.ProtoExcludePaths)
	run_configuration.SetTopic(topicConfig)

	encoder, err := p.getEncoder(produceCommand)
	if err != nil {
		common.ExitWithErr("%+v", err)
	}

	k.Produce(cmd.Context(), produceCommand, encoder)
}

func (p *produceOptions) getEncoder(cmd kafeman.ProduceCommand) (kafeman.Encoder, error) {
	topicConfig, ok := run_configuration.GetTopicByName(cmd.Topic)
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
		return serializers.NewProtobufSerializer(topicConfig.ProtoPaths, topicConfig.ProtoExcludePaths, topicConfig.ProtoType)
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
		return serializers.NewProtobufSerializer(topicConfig.ProtoPaths, topicConfig.ProtoExcludePaths, topicConfig.ProtoType)
	}

	// RAW DECODER
	return serializers.NewRawSerializer(), nil
}

func NewProduceCMD() *cobra.Command {
	options := newProduceOptions()

	cmd := &cobra.Command{
		Use:               "produce",
		Short:             "Produce record. Reads data from stdin.",
		Example:           "kafeman produce topic_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
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
