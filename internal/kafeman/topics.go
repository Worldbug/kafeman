package kafeman

import (
	"context"
	"errors"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/logger"
	"github.com/worldbug/kafeman/internal/models"
)

var ErrNoTopic = errors.New("No topic in cluster")

func (k *kafeman) GetTopicInfo(ctx context.Context, topic string) (models.Topic, error) {
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

	return topicInfo, err
}

func (k *kafeman) ListTopicConsumers(ctx context.Context, topic string) (models.TopicConsumers, error) {
	adm := admin.NewAdmin(k.config)
	consumers, err := adm.GetTopicConsumers(ctx, topic)

	return models.TopicConsumers{
		Consumers: consumers,
	}, err
}

func (k *kafeman) DeleteTopic(ctx context.Context, topic string) error {
	adm := admin.NewAdmin(k.config)
	err := adm.DeleteTopic(ctx, topic)
	if err != nil {
		logger.Errorf("Delete topic [%s] err: %+w", topic, err)
	}

	return err
}

type SetConfigValueTopicCommand struct {
	Topic  string
	Values map[string]string
}

func (k *kafeman) SetConfigValueTopic(ctx context.Context, command SetConfigValueTopicCommand) error {
	adm := admin.NewAdmin(k.config)

	err := adm.ConfigureTopic(ctx, command.Topic, command.Values)
	if err != nil {
		logger.Errorf("Delete topic [%s]\nvalues:\n%+v\n err: %+v",
			command.Topic, command.Values, err)
	}

	return err
}
