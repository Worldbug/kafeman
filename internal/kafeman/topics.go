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
		logger.Errorf("Delete topic [%s] err: %+v", topic, err)
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

type UpdateTopicCommand struct {
	Topic           string
	PartitionsCount int32
	// partiton -> offset
	Assignments [][]int32
}

func (k *kafeman) UpdateTopic(ctx context.Context, command UpdateTopicCommand) error {
	adm := admin.NewAdmin(k.config)
	err := adm.UpdateTopic(ctx,
		command.Topic,
		command.PartitionsCount,
		command.Assignments,
	)

	if err != nil {
		logger.Errorf("Update topic [%s]\n partitions: %+v\nassignments: %+v err: %+v",
			command.Topic, command.PartitionsCount, command.Assignments, err,
		)
	}

	return err
}

type CreateTopicCommand struct {
	TopicName         string
	PartitionsCount   int32
	ReplicationFactor int16
	CleanupPolicy     string
}

func (k *kafeman) CreateTopic(ctx context.Context, command CreateTopicCommand) error {
	adm := admin.NewAdmin(k.config)
	err := adm.CreateTopic(ctx,
		command.TopicName,
		command.PartitionsCount,
		command.ReplicationFactor,
		command.CleanupPolicy,
	)
	if err != nil {
		logger.Errorf("Create topic  [%+v]\nPartitions: %+v\nReplication Factor: %+v\nCleanupPolicy: %+v  err: %+v",
			command.TopicName, command.PartitionsCount,
			command.ReplicationFactor, command.CleanupPolicy, err)

	}

	return err
}

type AddConfigRecordCommand struct {
	Topic, Key, Value string
}

func (k *kafeman) AddConfigRecord(ctx context.Context, command AddConfigRecordCommand) error {
	adm := admin.NewAdmin(k.config)

	err := adm.AddConfig(ctx, command.Topic, command.Key, command.Value)
	if err != nil {
		logger.Errorf("Add config record for topi [%+v] %+v=%+v err: %+v",
			command.Topic, command.Key, command.Value, err,
		)
	}

	return err
}
