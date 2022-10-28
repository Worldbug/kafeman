package kafeman

import (
	"context"
	"fmt"
	"io"
	"kafeman/internal/config"
	"kafeman/internal/consumer"
	"kafeman/internal/proto"
	"sync"

	"github.com/Shopify/sarama"
)

func Newkafeman(
	config config.Config,
	outWriter io.Writer,
	errWriter io.Writer,
	// TODO: remove
	// inReader  io.Reader,
) *kafeman {

	return &kafeman{
		config:    config,
		outWriter: outWriter,
		errWriter: errWriter,
		// TODO: remove
		// protoDecoder: *proto.NewProtobufDecoder(config.Protobuf.ProtoPaths),
		// TODO: remove
		// inReader  : inReader,
	}
}

type kafeman struct {
	config config.Config

	consumer consumer.Consumer

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader

	protoDecoder proto.ProtobufDecoder
}

func (pk *kafeman) Consume(ctx context.Context, topic, consumerGroup string, partitions []int32, markMessages bool, offset int64) {
	// TODO: remove
	pk.protoDecoder = *proto.NewProtobufDecoder(pk.config.Topics[topic].ProtoPaths)
	// FIXME:
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c := consumer.NewConsumer(topic, consumerGroup, partitions, markMessages, offset, pk.config.GetCurrentCluster().Brokers)
	messages := c.Consume(ctx)
	if protoType := pk.config.Topics[topic].ProtoType; protoType != "" {
		pk.handleProtoMessages(messages, protoType)
	}
	wg.Wait()
}

func (pk *kafeman) handleProtoMessages(messages chan *sarama.ConsumerMessage, protoType string) {
	for msg := range messages {
		data, err := pk.protoDecoder.DecodeProto(msg.Value, protoType)
		if err != nil {
			// TODO: log error
			continue
		}

		fmt.Fprintln(pk.outWriter, string(data))
	}

}
