package kafeman

import (
	"time"
)

type Message struct {
	Headers   map[string]string
	Timestamp time.Time
	// TODO: remove
	BlockTimestamp time.Time

	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

type PrintableMessage struct {
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp,omitempty"`

	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition,omitempty"`
	Offset    int64  `json:"offset,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

func messageToPrintable(msg Message) PrintableMessage {
	return PrintableMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,

		Headers:   msg.Headers,
		Timestamp: msg.Timestamp,

		Key:   string(msg.Key),
		Value: string(msg.Value),
	}
}

type Topic struct {
	Name       string
	Partitions int
	Replicas   int
}
