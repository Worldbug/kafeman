package protokaf

import (
	"context"
	"fmt"
	"io"
	"protokaf/internal/config"
	"protokaf/internal/consumer"
	"protokaf/internal/proto"
	"sync"

	"github.com/Shopify/sarama"
)

func NewProtokaf(
	config config.Config,
	outWriter io.Writer,
	errWriter io.Writer,
	// TODO: remove
	// inReader  io.Reader,
) *Protokaf {

	return &Protokaf{
		config:       config,
		outWriter:    outWriter,
		errWriter:    errWriter,
		protoDecoder: *proto.NewProtobufDecoder(config.Protobuf.ProtoPaths),
		// TODO: remove
		// inReader  : inReader,
	}
}

type Protokaf struct {
	config config.Config

	consumer consumer.Consumer

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader

	protoDecoder proto.ProtobufDecoder
}

func (pk *Protokaf) Consume(ctx context.Context, topic, consumerGroup string, partitions []int32, markMessages bool, offset int64) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c := consumer.NewConsumer(topic, consumerGroup, partitions, markMessages, offset, pk.config.GetCurrentCluster().Brokers)
	messages := c.Consume(ctx)
	if protoType := pk.config.Protobuf.Topics[topic]; protoType != "" {
		pk.handleProtoMessages(messages, protoType)
	}
	wg.Wait()
}

func (pk *Protokaf) handleProtoMessages(messages chan *sarama.ConsumerMessage, protoType string) {
	for msg := range messages {
		data, err := pk.protoDecoder.DecodeProto(msg.Value, protoType)
		if err != nil {
			// TODO: log error
			continue
		}

		fmt.Fprintln(pk.outWriter, string(data))
	}

}
