package admin

import (
	"context"

	"github.com/Shopify/sarama"
)

func (a *Admin) DeleteTopic(ctx context.Context, topic string) error {
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return err
	}
	return adm.DeleteTopic(topic)
}

type TopicConfig map[string]sarama.IncrementalAlterConfigsEntry

func (a *Admin) ConfigureTopic(ctx context.Context, topic string, config map[string]string) error {
	configs := make(map[string]sarama.IncrementalAlterConfigsEntry)

	for key, value := range config {
		configs[key] = sarama.IncrementalAlterConfigsEntry{
			Operation: sarama.IncrementalAlterConfigsOperationSet,
			Value:     &value,
		}
	}

	adm, err := a.getSaramaAdmin()
	if err != nil {
		return err
	}
	return adm.IncrementalAlterConfig(sarama.TopicResource, topic, configs, false)
}

func (a *Admin) UpdateTopic(ctx context.Context, topic string, partitionsCount int32, assignments [][]int32) error {
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return err
	}

	if partitionsCount != -1 {
		return adm.CreatePartitions(topic, partitionsCount, assignments, false)
	}

	return adm.AlterPartitionReassignments(topic, assignments)
}

func (a *Admin) CreateTopic(ctx context.Context,
	topicName string,
	partitionsCount int32,
	replicationFactor int16,
	cleanupPolicy string,
) error {
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return err
	}
	return adm.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     partitionsCount,
		ReplicationFactor: replicationFactor,
		ConfigEntries: map[string]*string{
			"cleanup.policy": &cleanupPolicy,
		},
	}, false)
}

func (a *Admin) AddConfig(ctx context.Context, topic, key, value string) error {
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return err
	}

	return adm.AlterConfig(sarama.TopicResource, topic, map[string]*string{
		key: &value,
	}, false)
}
