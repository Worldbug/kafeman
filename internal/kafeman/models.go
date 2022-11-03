package kafeman

import (
	"time"

	"github.com/Shopify/sarama"
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

// TODO: headers
func messageFromSarama(msg *sarama.ConsumerMessage) Message {
	return Message{
		Timestamp: msg.Timestamp,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
	}
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

func messageToPrintable(msg Message) PrintableMessage {
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

type Topic struct {
	Name       string
	Partitions int
	Replicas   int
}

func NewGroup() Group {
	return Group{
		Members: make([]Member, 0),
		Offsets: make(map[string][]Offset),
	}
}

type Group struct {
	GroupID string `json:"group_id"`
	State   string `json:"state"`
	/// TODO:
	Offsets map[string][]Offset `json:"offsets"`
	// Protocol     string
	// ProtocolType string
	Members []Member `json:"members"`
}

type Member struct {
	Host        string       `json:"host"`
	ID          string       `json:"id"`
	Assignments []Assignment `json:"assignments"`
	// MetaData
}

type Assignment struct {
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

type Offset struct {
	Partition      int32 `json:"partition"`
	Offset         int64 `json:"offset"`
	HightWatermark int64 `json:"hight_watermark"`
	Lag            int64 `json:"lag"`
	// Metadata
}
