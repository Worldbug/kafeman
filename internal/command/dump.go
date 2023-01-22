package command

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/worldbug/kafeman/internal/kafeman"
	"github.com/worldbug/kafeman/internal/logger"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/serializers"
)

func init() {
	RootCMD.AddCommand(dumpCMD)
}

type RestorableMessage struct {
	PrintableMessage `json:"message"`
	Payload          string `json:"payload"`
}

var dumpCMD = &cobra.Command{
	Use:               "dump",
	Short:             "Consume messages from kafka topic",
	Example:           "kafeman consume topic_name",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: topicCompletion,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		offset := getOffsetFromFlag()
		topic := args[0]

		k := kafeman.Newkafeman(conf)

		command := kafeman.DumpCommand{
			ConsumeCommand: kafeman.ConsumeCommand{
				Topic:          topic,
				ConsumerGroup:  groupIDFlag,
				Partitions:     partitionsFlag,
				Offset:         offset,
				CommitMessages: commitFlag,
				Follow:         followFlag,
				WithMeta:       printMetaFlag,
				MessagesCount:  messagesCountFlag,
				FromTime:       parseTime(fromAtFlag),
			},
		}

		decoder, err := getDecoder(command.ConsumeCommand)
		if err != nil {
			errorExit("%+v", err)
		}

		messages, err := k.Dump(cmd.Context(), command, decoder)
		if err != nil {
			errorExit("%+v", err)
		}

		for message := range messages {
			PrintDump(message)
		}
	},
}

func PrintDump(message models.Message) {
	buf := bytes.Buffer{}
	gob.NewEncoder(&buf).Encode(message)
	payload, err := serializers.NewBase64Serializer().Encode(buf.Bytes())
	if err != nil {
		logger.Fatalf("Can`t encode payload: %+v", err)
		return
	}

	restorable := RestorableMessage{
		Payload:          string(payload),
		PrintableMessage: messageToPrintable(message),
	}

	if isJSON(message.Value) {
		v := restorable.PrintableMessage.Value
		restorable.PrintableMessage.Value = ""
		msg, err := json.Marshal(restorable)
		if err != nil {
			logger.Fatalf("Can`t marshall dumped message: %+v", err)
			return
		}
		m := strings.Replace(string(msg), `"value":""`, fmt.Sprintf(`"value":%v`, v), 1)
		fmt.Fprintln(outWriter, m)
		return
	}

	msg, err := json.Marshal(restorable)
	if err != nil {
		logger.Fatalf("Can`t marshall dumped message: %+v", err)
		return
	}

	fmt.Fprintln(outWriter, string(msg))
}
