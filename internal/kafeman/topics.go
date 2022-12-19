package kafeman

import (
	"context"
	"errors"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/models"
)

var ErrNoTopic = errors.New("No topic in cluster")

func (k *kafeman) GetTopicInfo(ctx context.Context, topic string) (models.Topic, error) {
	// TODO: decribe topic
	topics, err := k.ListTopics(ctx)
	if err != nil {
		return models.Topic{}, err
	}

	for _, t := range topics {
		if t.Name == topic {
			return t, nil
		}
	}

	return models.Topic{}, ErrNoTopic
}

func (k *kafeman) DescribeTopic(ctx context.Context, topic string) (models.TopicInfo, error) {
	adm := admin.NewAdmin(k.config)
	topicInfo, err := adm.DescribeTopic(ctx, topic)
	if err != nil {
		return topicInfo, err
	}

	topicInfo.Consumers, err = adm.GetTopicConsumers(ctx, topic)

	return topicInfo, err
}
