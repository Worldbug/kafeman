package kafeman

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kafeman/internal/config"
	"kafeman/internal/consumer"
	"kafeman/internal/proto"
	"sync"
	"time"

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

type RunConfig struct {
	Topic         string
	ConsumerGroup string
	Partitions    []int32
	MarkMessages  bool
	Offset        int64
	Follow        bool
	WithKey       bool
}

func (pk *kafeman) Consume(ctx context.Context, setup RunConfig) {
	// TODO: remove
	pk.protoDecoder = *proto.NewProtobufDecoder(pk.config.Topics[setup.Topic].ProtoPaths)
	// FIXME:
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c := consumer.NewConsumer(setup.Topic, setup.ConsumerGroup, setup.Partitions, setup.MarkMessages, setup.Offset, pk.config.GetCurrentCluster().Brokers)
	messages := c.Consume(ctx)
	if protoType := pk.config.Topics[setup.Topic].ProtoType; protoType != "" {
		pk.handleProtoMessages(messages, protoType, setup)
	}
	wg.Wait()
}

func (pk *kafeman) handleProtoMessages(messages chan *sarama.ConsumerMessage, protoType string, setup RunConfig) {
	for msg := range messages {
		data, err := pk.protoDecoder.DecodeProto(msg.Value, protoType)
		if err != nil {
			// TODO: log error
			continue
		}

		if setup.WithKey {
			pk.Print(Message{
				Timestamp:      msg.Timestamp,
				BlockTimestamp: msg.BlockTimestamp,
				Topic:          msg.Topic,
				Offset:         msg.Offset,

				Key:   string(msg.Key),
				Value: string(data),
			})
			continue
		}

		fmt.Fprintln(pk.outWriter, string(data))
	}

}

func (pk *kafeman) Print(data Message) {
	msg, _ := json.Marshal(data)
	fmt.Fprintln(pk.outWriter, string(msg))
}

type Message struct {
	Headers        []string  `json:"headers,omitempty"`
	Timestamp      time.Time `json:"timestamp,omitempty"`
	BlockTimestamp time.Time `json:"block_timestamp,omitempty"`

	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition,omitempty"`
	Offset    int64  `json:"offset,omitempty"`
}
