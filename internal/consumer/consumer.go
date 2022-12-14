package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/config"

	"github.com/worldbug/kafeman/internal/models"

	"github.com/Shopify/sarama"
)

func NewSaramaConsuemr(
	config config.Config,
	consumerGroupID string,
	topic string,
	partitions []int32,
	offset int64,
	messagesLimit int32,
	commit bool,
	follow bool,
	fromTime time.Time,
) *Consumer {
	return &Consumer{
		config:          config,
		consumerGroupID: consumerGroupID,
		topic:           topic,
		partitions:      partitions,
		offset:          offset,
		messagesLimit:   messagesLimit,
		commit:          commit,
		follow:          follow,
		fromTime:        fromTime,
	}
}

type Consumer struct {
	config   config.Config
	messages chan models.Message

	consumerGroupID string
	topic           string
	partitions      []int32
	offset          int64
	messagesLimit   int32
	commit          bool
	follow          bool
	fromTime        time.Time
}

func (c *Consumer) StartConsume(ctx context.Context) (<-chan models.Message, error) {
	if c.consumerGroupID != "" {
		return c.consumerGroup(ctx)
	}

	return c.consumer(ctx)
}

func (c *Consumer) consumer(ctx context.Context) (<-chan models.Message, error) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig := c.getSaramaConfig()

	consumer, err := sarama.NewConsumer(addrs, saramaConfig)
	if err != nil {
		return nil, err
	}
	// set outputRate

	if len(c.partitions) == 0 {
		c.partitions, err = consumer.Partitions(c.topic)
		if err != nil {
			return nil, err
		}
	}

	c.messages = make(chan models.Message, len(c.partitions))
	go c.asyncConsumersWorkGroup(ctx, consumer, c.topic, c.partitions)

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
			offset := c.offset

			if c.fromTime.UnixNano() != 0 {
				offset = adm.GetOffsetByTime(ctx, partition, topic, c.fromTime)
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
	left := c.messagesLimit

	for {
		select {
		case msg, ok := <-cp.Messages():
			if !ok {
				return nil
			}

			c.messages <- models.MessageFromSarama(msg)

			if !c.follow && left == 0 {
				lastOffset := cp.HighWaterMarkOffset()
				currentOffset := msg.Offset
				if lastOffset-currentOffset-1 == 0 {
					return nil
				}
			}

			if c.messagesLimit != 0 {
				left--
			}

			if c.messagesLimit != 0 && left == 0 {
				return nil
			}

		}
	}

}

func (c *Consumer) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = c.commit

	if c.offset == sarama.OffsetNewest || c.offset == sarama.OffsetOldest {
		saramaConfig.Consumer.Offsets.Initial = c.offset
	}

	return saramaConfig
}

func (c *Consumer) consumerGroup(ctx context.Context) (<-chan models.Message, error) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig := c.getSaramaConfig()
	topic := c.topic

	// defer cg.close
	cg, err := sarama.NewConsumerGroup(addrs, c.consumerGroupID, saramaConfig)
	if err != nil {
		return nil, err
	}

	cli, err := sarama.NewClient(c.config.GetCurrentCluster().Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	if len(c.partitions) == 0 {
		c.partitions, err = cli.Partitions(topic)
		if err != nil {
			return nil, err
		}
	}

	c.messages = make(chan models.Message, len(c.partitions))
	go cg.Consume(ctx, []string{topic}, c)

	return c.messages, nil
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	close(c.messages)
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	left := c.messagesLimit

	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				continue
			}

			c.messages <- models.MessageFromSarama(msg)

			if !c.follow {
				lastOffset := claim.HighWaterMarkOffset()
				currentOffset := msg.Offset
				if lastOffset-currentOffset-1 == 0 {
					return nil
				}

			}

			if c.messagesLimit != 0 {
				left--
			}

			if c.messagesLimit != 0 && left == 0 {
				return nil
			}

		}
	}
}
