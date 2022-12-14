package admin

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/worldbug/kafeman/internal/config"

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

func (a *Admin) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true

	return saramaConfig
}

func (a *Admin) GetOffsetByTime(ctx context.Context, partition int32, topic string, ts time.Time) int64 {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: a.config.GetCurrentCluster().Brokers,
		Topic:   topic,
	})

	err := reader.SetOffsetAt(ctx, ts)
	if err != nil {
		return -1
	}

	return reader.Offset()

}

func (a *Admin) client() kafka.Client {
	return kafka.Client{
		Addr:    kafka.TCP(a.config.GetCurrentCluster().Brokers...),
		Timeout: time.Second * 15,
	}
}

var ErrNoBrokers = errors.New("Empty brokers list")

func (a *Admin) conn() (*kafka.Conn, error) {
	if len(a.config.GetCurrentCluster().Brokers) < 1 ||
		len(a.config.GetCurrentCluster().Brokers[0]) < 1 {
		return nil, ErrNoBrokers
	}

	return kafka.Dial("tcp", a.config.GetCurrentCluster().Brokers[0])
}
func (a *Admin) getSaramaAdmin() sarama.ClusterAdmin {
	var admin sarama.ClusterAdmin
	clusterAdmin, err := sarama.NewClusterAdmin(a.config.GetCurrentCluster().Brokers, a.getSaramaConfig())
	if err != nil {
		return admin
	}

	return clusterAdmin
}

func (a *Admin) asyncGetLastOffset(ctx context.Context, wg *sync.WaitGroup, mu *sync.Mutex, offsetMap map[string]map[int]int64, topic string, parts ...int) error {
	defer wg.Done()
	for _, partition := range parts {
		offset, err := a.fetchLastOffset(ctx, topic, partition)
		if err != nil {
			return err
		}
		mu.Lock()
		offsetMap[topic][int(offset.Partition)] = offset.HightWatermark
		mu.Unlock()
	}

	return nil
}
