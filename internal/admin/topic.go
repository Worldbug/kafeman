package admin

import (
	"context"

	"github.com/Shopify/sarama"
)

func (a *Admin) DeleteTopic(ctx context.Context, topic string) error {
	adm := a.getSaramaAdmin()
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

	adm := a.getSaramaAdmin()
	return adm.IncrementalAlterConfig(sarama.TopicResource, topic, configs, false)
}
