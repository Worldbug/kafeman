package protokaf

import (
	"context"
	"fmt"
	"io"
	"protokaf/internal/config"
	"protokaf/internal/consumer"
	"protokaf/internal/proto"

	"github.com/Shopify/sarama"
)

func NewProtokaf(
	config config.Cluster,
	outWriter io.Writer,
	errWriter io.Writer,
	// TODO: remove
	// inReader  io.Reader,
) *Protokaf {
	return &Protokaf{
		config:    config,
		outWriter: outWriter,
		errWriter: errWriter,
		// TODO: remove
		// inReader  : inReader,
	}
}

type Protokaf struct {
	config config.Cluster

	consumer consumer.Consumer
	brokers  []string

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader

	protoDecoder proto.ProtobufDecoder
}

func (pk *Protokaf) Consume(ctx context.Context, topic, consumerGroup string, partitions []int32, markMessages bool, offset int64) {
	c := consumer.NewConsumer(topic, consumerGroup, partitions, markMessages, offset, pk.brokers)
	messages := c.Consume(ctx)
	if pk.config.ProtoType != "" {

		pk.handleProtoMessages(messages)
	}
}

func (pk *Protokaf) handleProtoMessages(messages chan *sarama.ConsumerMessage) {
	for msg := range messages {
		data, err := pk.protoDecoder.DecodeProto(msg.Value, pk.config.ProtoType)
		if err != nil {
			// TODO: log error
			continue
		}

		fmt.Fprintln(pk.outWriter, data)
	}

}
