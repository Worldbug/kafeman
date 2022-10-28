package kafeman

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kafeman/internal/config"
	"kafeman/internal/consumer"
	"kafeman/internal/proto"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/segmentio/kafka-go"
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
	WithMeta      bool
}

// TOD): refactor
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

	k.handleMessage(messages, cmd)
	wg.Wait()
}

func (k *kafeman) handleProtoMessages(messages chan *sarama.ConsumerMessage, protoType string, setup ConsumeCommand) {
	for msg := range messages {
		data, err := k.protoDecoder.DecodeProto(msg.Value, protoType)
		if err != nil {
			fmt.Fprintln(k.errWriter, err)
			continue
		}

		if setup.WithMeta {
			k.Print(Message{
				Timestamp:      msg.Timestamp,
				BlockTimestamp: msg.BlockTimestamp,
				Topic:          msg.Topic,
				Offset:         msg.Offset,

				Key:   string(msg.Key),
				Value: data,
			})
			continue
		}

		fmt.Fprintln(k.outWriter, string(data))
	}

}

func (k *kafeman) handleMessage(messages chan *sarama.ConsumerMessage, setup ConsumeCommand) {
	for msg := range messages {
		if setup.WithMeta {
			k.Print(Message{
				Timestamp:      msg.Timestamp,
				BlockTimestamp: msg.BlockTimestamp,
				Topic:          msg.Topic,
				Offset:         msg.Offset,

				Key:   string(msg.Key),
				Value: msg.Value,
			})
			continue
		}

		fmt.Fprintln(k.outWriter, string(msg.Value))
	}
}

// TODO: Поправить этот костыль
func (k *kafeman) Print(data Message) {
	b := data.Value
	data.Value = []byte{}
	msg, _ := json.Marshal(data)

	m := strings.Replace(string(msg), "\"value\":\"\"", "\"value\":"+string(b), 1)

	fmt.Fprintln(k.outWriter, m)
}

type Message struct {
	Headers        []string  `json:"headers,omitempty"`
	Timestamp      time.Time `json:"timestamp,omitempty"`
	BlockTimestamp time.Time `json:"block_timestamp,omitempty"`

	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition,omitempty"`
	Offset    int64  `json:"offset,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     []byte `json:"value"`
}

type Topic struct {
	Name       string
	Partitions int
	Replicas   int
}

func (k *kafeman) ListTopics(ctx context.Context) []Topic {
	if len(k.config.GetCurrentCluster().Brokers[0]) < 1 {
		return []Topic{}
	}

	conn, err := kafka.Dial("tcp", k.config.GetCurrentCluster().Brokers[0])
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	topics := map[string]Topic{}

	for _, p := range partitions {
		if info, ok := topics[p.Topic]; ok {
			info.Partitions++
			info.Replicas += len(p.Replicas)
			topics[p.Topic] = info
			continue
		}

		topics[p.Topic] = Topic{
			Name:       p.Topic,
			Partitions: 1,
			Replicas:   len(p.Replicas),
		}
	}

	sortedTopics := make([]Topic, len(topics))

	i := 0
	for _, topic := range topics {
		sortedTopics[i] = topic
		i++
	}

	sort.Slice(sortedTopics, func(i int, j int) bool {
		return sortedTopics[i].Name < sortedTopics[j].Name
	})

	return sortedTopics
}
