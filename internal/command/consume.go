package command

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/spf13/cobra"
)

const (
	newestOffset = -1
	oldestOffset = -2
)

var (
	protoFiles   []string
	protoExclude []string
	protoType    string
	encoding     string

	offsetFlag        string
	groupIDFlag       string
	partitionsFlag    []int32
	followFlag        bool
	commitFlag        bool
	printMetaFlag     bool
	fromAtFlag        string
	messagesCountFlag int32
)

func init() {
	RootCMD.AddCommand(ConsumeCMD)

	ConsumeCMD.Flags().StringVar(&offsetFlag, "offset", "oldest", "Offset to start consuming. Possible values: oldest (-2), newest (-1), or integer. Default oldest")
	ConsumeCMD.RegisterFlagCompletionFunc("offset", offsetCompletion)
	ConsumeCMD.Flags().StringVar(&encoding, "force-encoding", "", "Fore set encoding one of [raw,proto,avro,msgpack,base64]")
	ConsumeCMD.RegisterFlagCompletionFunc("force-encoding", encodingCompletion)
	ConsumeCMD.Flags().StringVarP(&groupIDFlag, "group", "g", "", "Consumer Group ID to use for consume")
	ConsumeCMD.RegisterFlagCompletionFunc("group", groupCompletion)
	ConsumeCMD.Flags().BoolVarP(&followFlag, "follow", "f", false, "Continue to consume messages until program execution is interrupted/terminated")
	ConsumeCMD.Flags().BoolVar(&commitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")
	ConsumeCMD.Flags().BoolVar(&printMetaFlag, "meta", false, "Print with meta info (marshal into json)")
	ConsumeCMD.Flags().Int32SliceVarP(&partitionsFlag, "partitions", "p", []int32{}, "Partitions to consume")
	ConsumeCMD.Flags().Int32VarP(&messagesCountFlag, "tail", "n", 0, "Print last n messages per partition")
	ConsumeCMD.Flags().StringVar(&fromAtFlag, "from", "", "Consume messages earlier time (format 2022-10-30T00:00:00)")
	ConsumeCMD.RegisterFlagCompletionFunc("from", timeCompletion)
}

var ConsumeCMD = &cobra.Command{
	Use:               "consume",
	Short:             "Consume messages from kafka topic",
	Example:           "kafeman consume topic_name",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: topicCompletion,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		offset := getOffsetFromFlag()
		topic := args[0]

		k := kafeman.Newkafeman(conf)

		command := kafeman.ConsumeCommand{
			Topic:          topic,
			ConsumerGroup:  groupIDFlag,
			Partitions:     partitionsFlag,
			Offset:         offset,
			CommitMessages: commitFlag,
			Follow:         followFlag,
			WithMeta:       printMetaFlag,
			MessagesCount:  messagesCountFlag,
			FromTime:       parseTime(fromAtFlag),
		}

		decoder, err := getDecoder(command)
		if err != nil {
			errorExit("%+v", err)
		}

		messages, err := k.Consume(cmd.Context(), command, decoder)
		if err != nil {
			errorExit("%+v", err)
		}

		for message := range messages {
			printMessage(message, printMetaFlag)
		}
	},
}

func getDecoder(cmd kafeman.ConsumeCommand) (kafeman.Decoder, error) {
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

func printMessage(message models.Message, printMeta bool) {
	if !printMeta {
		fmt.Fprintln(outWriter, string(message.Value))
		return
	}

	Print(message)
}

func Print(data models.Message) {
	if isJSON(data.Value) {
		ms := messageToPrintable(data)
		v := ms.Value
		ms.Value = ""
		msg, _ := json.Marshal(ms)
		m := strings.Replace(string(msg), `"value":""`, fmt.Sprintf(`"value":%v`, v), 1)
		fmt.Fprintln(outWriter, m)
		return
	}

	msg, _ := json.Marshal(messageToPrintable(data))
	fmt.Fprintln(outWriter, string(msg))
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

func isJSON(data []byte) bool {
	var i interface{}
	if err := json.Unmarshal(data, &i); err == nil {
		return true
	}
	return false
}

func parseTime(str string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05", str)
	if err != nil {
		return time.Unix(0, 0)
	}

	return t.UTC()
}

var setupProtoDescriptorRegistry = func(cmd *cobra.Command, args []string) {
	if protoType != "" {
		r, err := serializers.NewDescriptorRegistry(protoFiles, protoExclude)
		if err != nil {
			errorExit("Failed to load protobuf files: %v\n", err)
		}

		protoRegistry = r
	}
}

func getOffsetFromFlag() int64 {
	var offset int64
	switch offsetFlag {
	case "oldest":
		offset = oldestOffset
	case "newest":
		offset = newestOffset
	default:
		o, err := strconv.ParseInt(offsetFlag, 10, 64)
		if err != nil {
			errorExit("Could not parse '%s' to int64: %v", offsetFlag, err)
		}
		offset = o
	}

	return offset
}
