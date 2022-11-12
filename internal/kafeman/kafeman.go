package kafeman

import (
	"context"
	"io"
	"kafeman/internal/config"
	"kafeman/internal/proto"
	"os"
	"sort"

	"github.com/segmentio/kafka-go"
)

func Newkafeman(
	config config.Config,
) *kafeman {

	return &kafeman{
		config:    config,
		outWriter: os.Stdout,
		errWriter: os.Stderr,
	}
}

type kafeman struct {
	config config.Config

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader

	protoDecoder proto.ProtobufDecoder
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
