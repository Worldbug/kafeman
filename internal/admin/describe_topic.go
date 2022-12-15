package admin

import (
	"context"

	"github.com/worldbug/kafeman/internal/models"

	"github.com/Shopify/sarama"
)

/*
Это должно возвращать
консьюмеров лаг и оффсет
*/

func (a *Admin) DescribeTopic(ctx context.Context, topic string) (models.TopicInfo, error) {
	topicInfo := models.NewTopicInfo()

	info, err := a.getSaramaAdmin().DescribeConfig(
		sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic,
		})

	if err != nil {
		return topicInfo, err
	}

	for _, e := range info {
		topicInfo.Config = append(topicInfo.Config, models.TopicConfigRecord{
			Name:      e.Name,
			Value:     e.Value,
			ReadOnly:  e.ReadOnly,
			Sensitive: e.Sensitive,
		})

		// TODO: Это точно нужно ?
		if e.Name == "cleanup.policy" && e.Value == "compact" {
			topicInfo.Compacted = true
		}
	}

	// TODO:
	// topicInfo.Internal = detail.IsInternal
	partitions, err := a.describeTopicPartitons(ctx, topic)
	if err != nil {
		return topicInfo, err
	}

	topicInfo.Partitions = partitions

	return topicInfo, err
}

func (a *Admin) describeTopicPartitons(ctx context.Context, topic string) ([]models.PartitionInfo, error) {
	adm := a.getSaramaAdmin()

	topicDetails, err := adm.DescribeTopics([]string{topic})
	if err != nil {
		return nil, err
	}

	if len(topicDetails) == 0 || topicDetails[0].Err == sarama.ErrUnknownTopicOrPartition {
		return nil, err
	}

	partitonsInfo := make([]models.PartitionInfo, 0, len(topicDetails[0].Partitions))
	for _, partition := range topicDetails[0].Partitions {
		offsets := a.fetchLastOffset(ctx, topic, int(partition.ID))

		partitonsInfo = append(partitonsInfo, models.PartitionInfo{
			Partition:      partition.ID,
			HightWatermark: offsets.HightWatermark,
			Leader:         partition.Leader,
			Replicas:       len(partition.Replicas),
			ISR:            partition.Isr,
		})
	}

	return partitonsInfo, nil
}
