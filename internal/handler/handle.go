package handler

import (
	"encoding/json"
	"fmt"
	"github.com/worldbug/kafeman/internal/models"
	"strings"
	"time"
)

func (mg *MessageHandler) handleProtoMessages(message models.Message, protoType string) models.Message {
	data, err := mg.protoDecoder.DecodeProto(message.Value, protoType)
	if err != nil {
		fmt.Fprintln(mg.errWriter, err)
		return message
	}

	message.Value = data
	return message
}

func (mg *MessageHandler) printMessage(message models.Message, printMeta bool) {
	if !printMeta {
		fmt.Fprintln(mg.outWriter, string(message.Value))
		return
	}

	mg.Print(message)
}

func (mg *MessageHandler) Print(data models.Message) {
	if isJSON(data.Value) {
		ms := messageToPrintable(data)
		v := ms.Value
		ms.Value = ""
		msg, _ := json.Marshal(ms)
		m := strings.Replace(string(msg), `"value":""`, fmt.Sprintf(`"value":%v`, v), 1)
		fmt.Fprintln(mg.outWriter, m)
		return
	}

	msg, _ := json.Marshal(messageToPrintable(data))
	fmt.Fprintln(mg.outWriter, string(msg))
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
