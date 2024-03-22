package admin

import (
	"context"
	"sync"

	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/utils"

	"github.com/IBM/sarama"
)

func (a *Admin) DescribeTopic(ctx context.Context, topic string) (models.TopicInfo, error) {
	topicInfo := models.NewTopicInfo()
	topicInfo.TopicName = topic
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return topicInfo, err
	}

	info, err := adm.DescribeConfig(
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

	// TODO: topicInfo.Internal = detail.IsInternal
	partitions, err := a.describeTopicPartitons(ctx, topic)
	if err != nil {
		return topicInfo, err
	}

	topicInfo.Partitions = partitions

	return topicInfo, err
}

func (a *Admin) describeTopicPartitons(ctx context.Context, topic string) ([]models.PartitionInfo, error) {
	adm, err := a.getSaramaAdmin()
	if err != nil {
		return nil, err
	}

	topicDetails, err := adm.DescribeTopics([]string{topic})
	if err != nil {
		return nil, err
	}

	if len(topicDetails) == 0 || topicDetails[0].Err == sarama.ErrUnknownTopicOrPartition {
		return nil, err
	}

	partitonsInfo := make([]models.PartitionInfo, 0, len(topicDetails[0].Partitions))
	for _, partition := range topicDetails[0].Partitions {
		offsets, err := a.fetchLastOffset(ctx, topic, int(partition.ID))
		if err != nil {
			return nil, err
		}

		partitonsInfo = append(partitonsInfo, models.PartitionInfo{
			Partition:      partition.ID,
			HightWatermark: offsets.HightWatermark,
			Leader:         partition.Leader,
			Replicas:       len(partition.Replicas),
			ISR:            partition.Isr,
			LogEndOffset:   offsets.Offset,
			LogStartOffset: offsets.LogStartOffset,
		})
	}

	return partitonsInfo, nil
}

func (a *Admin) GetTopicConsumers(ctx context.Context, topic string) ([]models.TopicConsumerInfo, error) {
	consumersList := make([]models.TopicConsumerInfo, 0)
	groupsList, err := a.GetGroupsList(ctx)
	if err != nil {
		return consumersList, err
	}

	consumers := make(map[string]*models.TopicConsumerInfo)

	wg := &sync.WaitGroup{}
	batches := utils.BatchesFromSlice(groupsList, 100)

	for _, batch := range batches {
		for _, group := range batch {
			wg.Add(1)
			go func(group string) {
				defer wg.Done()
				a.describeTopicConsumers(ctx, topic, group, consumers)
			}(group)
		}

		wg.Wait()
	}

	for _, consumer := range consumers {
		consumersList = append(consumersList, *consumer)
	}

	return consumersList, nil
}

func (a *Admin) describeTopicConsumers(ctx context.Context, topic string, group string, consumers map[string]*models.TopicConsumerInfo) {
	groupInfo := a.DescribeGroup(ctx, group)
	for _, memeber := range groupInfo.Members {
		for _, assign := range memeber.Assignments {
			if assign.Topic == topic {
				_, ok := consumers[group]
				if !ok {
					consumers[group] = &models.TopicConsumerInfo{
						Name: group,
					}
				}

				consumers[group].MembersCount++
			}
		}
	}
}
