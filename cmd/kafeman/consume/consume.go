package consume_cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/worldbug/kafeman/internal/command"
	completion_cmd "github.com/worldbug/kafeman/internal/command/completion"
	"github.com/worldbug/kafeman/internal/command/global_config"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/serializers"
	"github.com/worldbug/kafeman/internal/utils"

	"github.com/spf13/cobra"
)

func NewConsumeCMD() *cobra.Command {
	options := newConsumeOptions()

	cmd := &cobra.Command{
		Use:               "consume",
		Short:             "Consume messages from kafka topic",
		Example:           "kafeman consume topic_name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion_cmd.NewTopicCompletion(),
		Run:               options.run,
	}

	cmd.Flags().StringVar(&options.offset, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	cmd.RegisterFlagCompletionFunc("offset", completion_cmd.NewOffsetCompletion())
	cmd.Flags().StringVar(&options.encoding, "force-encoding", "", "Fore set encoding one of [raw,proto,avro,msgpack,base64]")
	cmd.RegisterFlagCompletionFunc("force-encoding", completion_cmd.NewEncodingCompletion())
	cmd.Flags().StringVarP(&options.groupID, "group", "g", "", "Consumer Group ID to use for consume")
	cmd.RegisterFlagCompletionFunc("group", completion_cmd.NewGroupCompletion())
	cmd.Flags().BoolVarP(&options.follow, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	cmd.Flags().BoolVar(&options.commit, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	cmd.Flags().BoolVar(&options.printMeta, "meta", false, "Print with meta info (marshal into json)")
	cmd.Flags().Int32SliceVarP(&options.partitions, "partitions", "p", []int32{}, "Partitions to consume")
	cmd.Flags().Int32VarP(&options.messagesCount, "tail", "n", 0, "Print last n messages per partition")
	cmd.Flags().StringVar(&options.fromAt, "from", "", "Consume messages earlier time (format 2022-10-30T00:00:00)")
	cmd.Flags().StringVar(&options.toAt, "to", "", "Consume messages until the specified time (format 2022-10-30T00:00:00)")
	cmd.RegisterFlagCompletionFunc("from", completion_cmd.NewTimeCompletion())
	cmd.RegisterFlagCompletionFunc("to", completion_cmd.NewTimeCompletion())
	cmd.Flags().StringSliceVar(&options.protoFiles, "proto-files", []string{}, "Protobuf files to use for decoding")
	cmd.Flags().StringSliceVar(&options.protoExclude, "proto-exclude", []string{}, "Exclude fields from decoding")
	cmd.Flags().StringVar(&options.protoType, "proto-type", "", "Protobuf message type to use for decoding")

	return cmd
}

func newConsumeOptions() *consumeOptions {
	return &consumeOptions{
		out: os.Stdout,
	}
}

type consumeOptions struct {
	offset        string
	groupID       string
	partitions    []int32
	follow        bool
	commit        bool
	printMeta     bool
	fromAt        string
	toAt          string
	messagesCount int32

	// TODO: refactor
	out io.Writer

	encoding string

	// TODO: refactor
	protoFiles   []string
	protoExclude []string
	protoType    string
}

func (c *consumeOptions) run(cmd *cobra.Command, args []string) {
	topic := args[0]

	offset := command.GetOffsetFromFlag(c.offset)

	k := kafeman.Newkafeman(global_config.Config)

	topicConfig, _ := global_config.GetTopicByName(topic)
	topicConfig.ProtoType = utils.OrDefault(c.protoType, topicConfig.ProtoType)
	topicConfig.ProtoPaths = utils.OrDefaultSlice(c.protoFiles, topicConfig.ProtoPaths)
	topicConfig.ProtoExcludePaths = utils.OrDefaultSlice(c.protoExclude, topicConfig.ProtoExcludePaths)
	global_config.SetTopic(topicConfig)

	kafemanCommand := kafeman.ConsumeCommand{
		Topic:          topic,
		ConsumerGroup:  c.groupID,
		Partitions:     c.partitions,
		Offset:         offset,
		CommitMessages: c.commit,
		Follow:         c.follow,
		WithMeta:       c.printMeta,
		MessagesCount:  c.messagesCount,
		FromTime:       command.ParseTime(c.fromAt),
		ToTime:         command.ParseTime(c.toAt),
	}

	decoder, err := c.getDecoder(kafemanCommand)
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	messages, err := k.Consume(cmd.Context(), kafemanCommand, decoder)
	if err != nil {
		command.ExitWithErr("%+v", err)
	}

	for message := range messages {
		printMessage(message, c.out, c.printMeta)
	}
}

func (c *consumeOptions) getDecoder(cmd kafeman.ConsumeCommand) (kafeman.Decoder, error) {
	topicConfig, ok := global_config.GetTopicByName(cmd.Topic)
	if !ok && c.encoding == "" {
		return serializers.NewRawSerializer(), nil
	}

	// override encoding
	if c.encoding != "" {
		topicConfig.Encoding = config.Encoding(c.encoding)
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

func printMessage(message models.Message, out io.Writer, printMeta bool) {
	if !printMeta {
		fmt.Fprintln(out, string(message.Value))
		return
	}

	Print(message, out)
}

func Print(data models.Message, out io.Writer) {
	if command.IsJSON(data.Value) {
		ms := messageToPrintable(data)
		v := ms.Value
		ms.Value = ""
		msg, _ := json.Marshal(ms)
		m := strings.Replace(string(msg), `"value":""`, fmt.Sprintf(`"value":%v`, v), 1)
		fmt.Fprintln(out, m)
		return
	}

	msg, _ := json.Marshal(messageToPrintable(data))
	fmt.Fprintln(out, string(msg))
}

type PrintableMessage struct {
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp,omitempty"`

	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

func messageToPrintable(msg models.Message) PrintableMessage {
	return PrintableMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,

		Headers:   msg.Headers,
		Timestamp: msg.Timestamp.UTC(),

		Key:   string(msg.Key),
		Value: string(msg.Value),
	}
}

// func (c *consumeOptions) setupProtoDescriptorRegistry(topic string) {
// 	topicConfig, _ := global_config.GetTopicByName(topic)
//
// 	protoFiles := topicConfig.ProtoPaths
// 	var protoExclude []string
//
// 	if len(c.protoFiles) != 0 {
// 		protoFiles = c.protoFiles
// 	}
//
// 	if len(c.protoExclude) != 0 {
// 		protoExclude = c.protoFiles
// 	}
//
// 	r, err := serializers.NewDescriptorRegistry(protoFiles, protoExclude)
// 	if err != nil {
// 		logger.Errorf("Failed to load protobuf files: %v\n", err)
// 	}
//
// 	c.protoRegistry = r
// }
