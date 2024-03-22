package admin

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/worldbug/kafeman/internal/sarama_config"
)

func (a *Admin) GetOffsetsByTime(ctx context.Context, topic string, ts time.Time) ([]int64, error) {
	offsets := make([]int64, 0)

	config, err := sarama_config.GetSaramaFromConfig(a.config)
	if err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(a.config.GetCurrentCluster().Brokers, config)
	if err != nil {
		return offsets, err
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return offsets, err
	}

	for _, partition := range partitions {
		offset, err := client.GetOffset(topic, partition, ts.UnixMilli())
		if err != nil {
			return offsets, err
		}

		offsets = append(offsets, offset)
	}

	return offsets, nil
}

func (a *Admin) GetOffsetByTime(ctx context.Context, partition int32, topic string, ts time.Time) (int64, error) {
	config, err := sarama_config.GetSaramaFromConfig(a.config)
	if err != nil {
		return 0, err
	}

	client, err := sarama.NewClient(a.config.GetCurrentCluster().Brokers, config)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	offset, err := client.GetOffset(topic, partition, ts.UnixMilli())
	if err != nil {
		return 0, err
	}

	return offset, nil
}
