package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/config"

	"github.com/worldbug/kafeman/internal/models"

	"github.com/Shopify/sarama"
	"github.com/worldbug/kafeman/internal/sarama_config"
)

func NewSaramaConsuemr(
	config *config.Configuration,
	consumerGroupID string,
	topic string,
	partitions []int32,
	offset int64,
	messagesLimit int32,
	commit bool,
	follow bool,
	fromTime time.Time,
	toTime time.Time,
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
		toTime:          toTime,
	}
}

type Consumer struct {
	config   *config.Configuration
	messages chan models.Message

	consumerGroupID string
	topic           string
	partitions      []int32
	offset          int64
	messagesLimit   int32
	commit          bool
	follow          bool
	fromTime        time.Time
	toTime          time.Time
}

func (c *Consumer) StartConsume(ctx context.Context) (<-chan models.Message, error) {
	if c.consumerGroupID != "" {
		return c.consumerGroup(ctx)
	}

	return c.consumer(ctx)
}

func (c *Consumer) consumer(ctx context.Context) (<-chan models.Message, error) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig, err := c.getSaramaConfig()
	if err != nil {
		return nil, err
	}

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

			c.asyncConsume(ctx, cp)
		}(p, wg)
	}

	wg.Wait()
	close(c.messages)
}

func (c *Consumer) asyncConsume(ctx context.Context, cp sarama.PartitionConsumer) error {
	left := c.messagesLimit

	// Do not read empty partition
	if cp.HighWaterMarkOffset() == 0 && !c.follow && left == 0 {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-cp.Messages():
			if !ok {
				return nil
			}

			if c.toTime.Unix() != 0 && msg.Timestamp.After(c.toTime) {
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

func (c *Consumer) getSaramaConfig() (*sarama.Config, error) {
	saramaConfig, err := sarama_config.GetSaramaFromConfig(c.config)
	if err != nil {
		return nil, err
	}

	saramaConfig.Consumer.Offsets.AutoCommit.Enable = c.commit
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Group.Session.Timeout = time.Second * 30
	saramaConfig.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	if c.offset == sarama.OffsetNewest || c.offset == sarama.OffsetOldest {
		saramaConfig.Consumer.Offsets.Initial = c.offset
	}

	return saramaConfig, nil
}

func (c *Consumer) consumerGroup(ctx context.Context) (<-chan models.Message, error) {
	addrs := c.config.GetCurrentCluster().Brokers
	saramaConfig, err := c.getSaramaConfig()
	if err != nil {
		return nil, err
	}

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
		c.partitions, err = cli.Partitions(c.topic)
		if err != nil {
			return nil, err
		}
	}

	c.messages = make(chan models.Message, len(c.partitions))
	go cg.Consume(ctx, []string{c.topic}, c)

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

			if c.toTime.Unix() != 0 && msg.Timestamp.After(c.toTime) {
				return nil
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
