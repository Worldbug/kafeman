package admin

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/worldbug/kafeman/internal/config"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
	"github.com/worldbug/kafeman/internal/sarama_config"
)

func NewAdmin(config *config.Configuration) *Admin {
	return &Admin{
		config: config,
	}
}

type Admin struct {
	config *config.Configuration
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

func (a *Admin) getSaramaAdmin() (sarama.ClusterAdmin, error) {
	var admin sarama.ClusterAdmin
	saramaConfig, err := sarama_config.GetSaramaFromConfig(a.config)
	if err != nil {
		return admin, err
	}

	return sarama.NewClusterAdmin(a.config.GetCurrentCluster().Brokers, saramaConfig)
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
