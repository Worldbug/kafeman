package admin

import (
	"context"
	"fmt"
	"kafeman/internal/config"
	"time"

	"github.com/Shopify/sarama"
	"github.com/segmentio/kafka-go"
)

func NewAdmin(config config.Config) *Admin {
	return &Admin{
		config: config,
	}
}

type Admin struct {
	config config.Config
}

func (a *Admin) GetOffsetByTime(ctx context.Context, partition int32, topic string, ts time.Time) int64 {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: a.config.GetCurrentCluster().Brokers,
		Topic:   topic,
	})

	err := reader.SetOffsetAt(ctx, ts)
	if err != nil {
		// TODO: handle
		return -1
	}

	return reader.Offset()

}

// TODO: work to slow
func (a *Admin) ListTopics(ctx context.Context) []string {
	addrs := a.config.GetCurrentCluster().Brokers
	admin, err := sarama.NewClusterAdmin(addrs, a.getSaramaConfig())
	if err != nil {
		//TODO: err
		return []string{}
	}

	topicsMap, err := admin.ListTopics()
	if err != nil {
		return []string{}
	}

	fmt.Println(topicsMap)

	return []string{}
}

func (a *Admin) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true

	return saramaConfig
}
