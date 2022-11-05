package consumer

import (
	"context"
	"kafeman/internal/admin"
	"kafeman/internal/config"
	"kafeman/internal/handler"
	"kafeman/internal/models"

	"github.com/Shopify/sarama"
)

func NewSaramaConsuemr(
	handler *handler.MessageHandler,
	config config.Config,
	command models.ConsumeCommand,
) *Consumer {
	return &Consumer{
		config:  config,
		command: command,
		handler: handler,
	}
}

type Consumer struct {
	config  config.Config
	command models.ConsumeCommand
	handler *handler.MessageHandler
}

func (c *Consumer) StartConsume(ctx context.Context) {
	if c.command.ConsumerGroup != "" {
		c.consumerGroup(ctx)
		return
	}

	c.consumer(ctx)
}

func (c *Consumer) consumerGroup(ctx context.Context) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig := c.getSaramaConfig()
	topic := c.command.Topic
	group := c.command.ConsumerGroup

	cg, err := sarama.NewConsumerGroup(addrs, group, saramaConfig)
	if err != nil {
		// TODO: ERROR handling
		return
	}
	// TODO: defer close

	cli, err := sarama.NewClient(c.config.GetCurrentCluster().Brokers, saramaConfig)
	if err != nil {
		// TODO: ERROR handling
		return
	}

	partitions, err := cli.Partitions(topic)
	if err != nil {
		// TODO: ERROR handling
		return
	}

	c.handler.InitInput(len(partitions))
	go cg.Consume(ctx, []string{topic}, c)
}

func (c *Consumer) consumer(ctx context.Context) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig := c.getSaramaConfig()
	topic := c.command.Topic

	consumer, err := sarama.NewConsumer(addrs, saramaConfig)
	if err != nil {
		// TODO: ERROR handling
		return
	}
	// set outputRate
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		// TODO: ERROR handling
		return
	}

	adm := admin.NewAdmin(c.config)

	c.handler.InitInput(len(partitions))

	for _, p := range partitions {
		// TODO: set offset per partition mode
		go func(partition int32) {
			offset := c.command.Offset

			if c.command.FromTime.UnixNano() != 0 {
				offset = adm.GetOffsetByTime(ctx, partition, topic, c.command.FromTime)
			}

			cp, e := consumer.ConsumePartition(topic, partition, offset)
			if e != nil {
				return
			}

			c.asyncConsume(cp)
		}(p)

	}

}

func (c *Consumer) asyncConsume(cp sarama.PartitionConsumer) error {
	defer c.handler.Close()
	left := c.command.MessagesCount

	for {
		select {
		case msg, ok := <-cp.Messages():
			if !ok {
				return nil
			}
			message := models.MessageFromSarama(msg)
			c.handler.Handle(message)

			if !c.command.Follow {
				lastOffset := cp.HighWaterMarkOffset()
				currentOffset := msg.Offset
				// TODO: correct offset
				if lastOffset-currentOffset == 0 {
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
	defer c.handler.Close()
	left := c.command.MessagesCount

	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				continue
			}

			message := models.MessageFromSarama(msg)
			c.handler.Handle(message)

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
