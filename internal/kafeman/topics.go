package kafeman

import (
	"context"
	"sync"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/models"
)

func (k *kafeman) GetTopicInfo(ctx context.Context, topic string) models.Topic {
	topics := k.ListTopics(ctx)

	for _, t := range topics {
		if t.Name == topic {
			return t
		}
	}

	return models.Topic{}
}

func (k *kafeman) DescribeTopic(ctx context.Context, topic string) models.TopicInfo {
	topicInfo, err := admin.NewAdmin(k.config).DescribeTopic(ctx, topic)
	// TODO: err
	if err != nil {
		return topicInfo
	}

	topicInfo.TopicName = topic

	groupsList, err := k.GetGroupsList(ctx)
	if err != nil {
		return topicInfo
	}

	consumers := make(map[string]*models.TopicConsumerInfo)

	wg := &sync.WaitGroup{}
	batches := batchesFromSlice(groupsList, 100)
	// TODO: refactor
	for _, batch := range batches {
		for _, group := range batch {
			wg.Add(1)
			go func(group string) {
				defer wg.Done()
				groupInfo := k.DescribeGroup(ctx, group)
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
			}(group)
		}

		wg.Wait()
	}

	for _, consumer := range consumers {
		topicInfo.Consumers =
			append(topicInfo.Consumers, *consumer)
	}

	return topicInfo
}
