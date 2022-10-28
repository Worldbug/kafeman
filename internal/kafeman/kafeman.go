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
	// inReader io.Reader,
) *kafeman {

	return &kafeman{
		config:    config,
		outWriter: outWriter,
		errWriter: errWriter,
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

type ConsumeCommand struct {
	Topic         string
	ConsumerGroup string
	Partitions    []int32
	MarkMessages  bool
	Offset        int64
	Follow        bool
	WithKey       bool
}

func (k *kafeman) Consume(ctx context.Context, cmd ConsumeCommand) {
	// TODO: remove
	k.protoDecoder = *proto.NewProtobufDecoder(k.config.Topics[cmd.Topic].ProtoPaths)
	// FIXME:
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c := consumer.NewConsumer(cmd.Topic, cmd.ConsumerGroup, cmd.Partitions, cmd.MarkMessages, cmd.Offset, k.config.GetCurrentCluster().Brokers)
	messages := c.Consume(ctx)
	if protoType := k.config.Topics[cmd.Topic].ProtoType; protoType != "" {
		k.handleProtoMessages(messages, protoType, cmd)
	}
	wg.Wait()
}

func (k *kafeman) handleProtoMessages(messages chan *sarama.ConsumerMessage, protoType string, setup ConsumeCommand) {
	for msg := range messages {
		data, err := k.protoDecoder.DecodeProto(msg.Value, protoType)
		if err != nil {
			fmt.Fprintln(k.errWriter, err)
			continue
		}

		if setup.WithKey {
			k.Print(Message{
				Timestamp:      msg.Timestamp,
				BlockTimestamp: msg.BlockTimestamp,
				Topic:          msg.Topic,
				Offset:         msg.Offset,

				Key:   string(msg.Key),
				Value: string(data),
			})
			continue
		}

		fmt.Fprintln(k.outWriter, string(data))
	}

}

func (k *kafeman) Print(data Message) {
	msg, _ := json.Marshal(data)
	fmt.Fprintln(k.outWriter, string(msg))
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
