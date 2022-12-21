package consumer

import (
	"context"
	"sync"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/config"

	"github.com/worldbug/kafeman/internal/models"

	"github.com/Shopify/sarama"
)

func NewSaramaConsuemr(
	config config.Config,
	command models.ConsumeCommand,
) *Consumer {
	return &Consumer{
		config:  config,
		command: command,
	}
}

type Consumer struct {
	config   config.Config
	command  models.ConsumeCommand
	messages chan models.Message
}

func (c *Consumer) StartConsume(ctx context.Context) (<-chan models.Message, error) {
	if c.command.ConsumerGroup != "" {
		return c.consumerGroup(ctx)
	}

	return c.consumer(ctx)
}

func (c *Consumer) consumerGroup(ctx context.Context) (<-chan models.Message, error) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig := c.getSaramaConfig()
	topic := c.command.Topic
	group := c.command.ConsumerGroup

	cg, err := sarama.NewConsumerGroup(addrs, group, saramaConfig)
	if err != nil {
		return nil, err
	}
	// TODO: defer close

	cli, err := sarama.NewClient(c.config.GetCurrentCluster().Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	partitions := c.command.Partitions

	if len(partitions) == 0 {
		partitions, err = cli.Partitions(topic)
		if err != nil {
			return nil, err
		}
	}

	c.messages = make(chan models.Message, len(partitions))
	go cg.Consume(ctx, []string{topic}, c)

	return c.messages, nil
}

func (c *Consumer) consumer(ctx context.Context) (<-chan models.Message, error) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig := c.getSaramaConfig()
	topic := c.command.Topic

	consumer, err := sarama.NewConsumer(addrs, saramaConfig)
	if err != nil {
		return nil, err
	}
	// set outputRate

	partitions := c.command.Partitions

	if len(partitions) == 0 {
		partitions, err = consumer.Partitions(topic)
		if err != nil {
			return nil, err
		}
	}

	c.messages = make(chan models.Message, len(partitions))
	go c.asyncConsumersWorkGroup(ctx, consumer, topic, partitions)

	return c.messages, nil

}

func (c *Consumer) asyncConsumersWorkGroup(ctx context.Context, consumer sarama.Consumer, topic string, partitions []int32) {
	adm := admin.NewAdmin(c.config)
	wg := &sync.WaitGroup{}
	for _, p := range partitions {
		// TODO: set offset per partition mode
		wg.Add(1)
		go func(partition int32, wg *sync.WaitGroup) {
			defer wg.Done()
			offset := c.command.Offset

			if c.command.FromTime.UnixNano() != 0 {
				offset = adm.GetOffsetByTime(ctx, partition, topic, c.command.FromTime)
			}

			cp, e := consumer.ConsumePartition(topic, partition, offset)
			if e != nil {
				return
			}

			c.asyncConsume(cp)
		}(p, wg)
	}

	wg.Wait()
	close(c.messages)
}

func (c *Consumer) asyncConsume(cp sarama.PartitionConsumer) error {
	left := c.command.MessagesCount

	for {
		select {
		case msg, ok := <-cp.Messages():
			if !ok {
				return nil
			}

			c.messages <- models.MessageFromSarama(msg)

			if !c.command.Follow && left == 0 {
				lastOffset := cp.HighWaterMarkOffset()
				currentOffset := msg.Offset
				if lastOffset-currentOffset-1 == 0 {
					return nil
				}
			}

			if c.command.MessagesCount != 0 {
				left--
			}

			if c.command.MessagesCount != 0 && left == 0 {
				return nil
			}

		}
	}

}

func (c *Consumer) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = c.command.CommitMessages

	if c.command.Offset == sarama.OffsetNewest || c.command.Offset == sarama.OffsetOldest {
		saramaConfig.Consumer.Offsets.Initial = c.command.Offset
	}

	return saramaConfig
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	left := c.command.MessagesCount

	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				continue
			}

			c.messages <- models.MessageFromSarama(msg)

			if !c.command.Follow {
				lastOffset := claim.HighWaterMarkOffset()
				currentOffset := msg.Offset
				if lastOffset-currentOffset-1 == 0 {
					return nil
				}

			}

			if c.command.MessagesCount != 0 {
				left--
			}

			if c.command.MessagesCount != 0 && left == 0 {
				return nil
			}

		}
	}

}
