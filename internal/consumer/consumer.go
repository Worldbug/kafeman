package consumer

import (
	"context"
	"kafeman/internal/config"
	"kafeman/internal/models"

	"github.com/Shopify/sarama"
)

func NewSaramaConsuemr(
	output chan<- models.Message,
	config config.Config,
	command models.ConsumeCommand,
) *Consumer {
	return &Consumer{
		output:  output,
		config:  config,
		command: command,
	}
}

type Consumer struct {
	output  chan<- models.Message
	config  config.Config
	command models.ConsumeCommand
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

	cg.Consume(ctx, []string{topic}, c)
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

	for _, p := range partitions {
		// TODO: set offset per partition mode
		_, e := consumer.ConsumePartition(topic, p, c.command.Offset)
		if e != nil {
			continue
		}
	}

}

func (c *Consumer) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Consumer.Offsets.Initial = c.command.Offset
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = c.command.CommitMessages
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
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			message := models.MessageFromSarama(msg)
			c.output <- message

			if !c.command.Follow {
				lastOffset := claim.HighWaterMarkOffset()
				currentOffset := msg.Offset
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
