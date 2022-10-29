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

// TODO: rename
func (k *kafeman) handleProtoMessagesV2(message Message, protoType string) Message {
	data, err := k.protoDecoder.DecodeProto(message.Value, protoType)
	if err != nil {
		// TODO: вынести наверх
		fmt.Fprintln(k.errWriter, err)

		return message
	}

	message.Value = data
	return message
}

func (k *kafeman) printMessage(message Message, printMeta bool) {
	if !printMeta {
		fmt.Fprintln(k.outWriter, string(message.Value))
		return
	}

	k.Print(message)
}

func (k *kafeman) handleMessage(messages chan *sarama.ConsumerMessage, setup ConsumeCommand) {
	for msg := range messages {
		if setup.WithMeta {
			k.Print(Message{
				Timestamp:      msg.Timestamp,
				BlockTimestamp: msg.BlockTimestamp,
				Topic:          msg.Topic,
				Offset:         msg.Offset,

				Key:   msg.Key,
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
	msg, _ := json.Marshal(messageToPrintable(data))

	m := strings.Replace(string(msg), "\"value\":\"\"", "\"value\":"+string(b), 1)
	fmt.Fprintln(k.outWriter, m)
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
