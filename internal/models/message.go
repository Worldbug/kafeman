package models

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
func MessageFromSarama(msg *sarama.ConsumerMessage) Message {
	return Message{
		Timestamp: msg.Timestamp,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
	}
}
