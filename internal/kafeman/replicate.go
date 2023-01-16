package kafeman

import (
	"context"
	"sync"
	"time"

	"github.com/worldbug/kafeman/internal/consumer"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/producer"
)

type ReplicateCMD struct {
	SourceTopic  string
	SourceBroker string
	DestTopic    string
	DestBroker   string

	Partition   int32
	Partitioner string

	Partitions     []int32
	CommitMessages bool
	Offset         int64
	Follow         bool
	WithMeta       bool
	MessagesCount  int32
	FromTime       time.Time
	ConsumerGroup  string
}

func (k *kafeman) Replicate(ctx context.Context, cmd ReplicateCMD) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	messages, err := k.initReplicateConsumer(ctx, cmd)
	if err != nil {
		return err
	}

	input := make(chan producer.Message)
	k.initReplicateProducer(ctx, cmd, wg, input)

	for message := range messages {
		input <- producer.Message{
			Key:   message.Key,
			Value: message.Value,
		}
	}
	close(input)

	return nil
}

func (k *kafeman) initReplicateConsumer(ctx context.Context, cmd ReplicateCMD) (<-chan models.Message, error) {
	consumerConfig := k.config
	consumerConfig.CurrentCluster = cmd.SourceBroker

	c := consumer.NewSaramaConsuemr(
		consumerConfig,
		cmd.ConsumerGroup,
		cmd.SourceTopic,
		cmd.Partitions,
		cmd.Offset,
		cmd.MessagesCount,
		cmd.CommitMessages,
		cmd.Follow,
		cmd.FromTime,
	)

	messages, err := c.StartConsume(ctx)
	if err != nil {
		return messages, ErrNoTopicProvided
	}

	return messages, nil
}

func (k *kafeman) initReplicateProducer(ctx context.Context, cmd ReplicateCMD, wg *sync.WaitGroup, input <-chan producer.Message) error {
	propducerConfig := k.config
	propducerConfig.CurrentCluster = cmd.DestBroker
	kafkaProducer, err := producer.NewProducer(propducerConfig, cmd.Partitioner, cmd.Partition, input)
	if err != nil {
		return err
	}

	wg.Add(1)
	go kafkaProducer.Produce(cmd.DestTopic, wg)
	return nil
}
