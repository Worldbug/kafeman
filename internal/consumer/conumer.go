package consumer

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
)

// TODO: map partitions on offset if offset not empty
func NewConsumer(topic, consumerGroup string, partitions []int32, markMessages bool, offset int64, brokers []string) Consumer {
	// TODO: перенести его выше
	client, err := sarama.NewClient(brokers, getConfig())
	if err != nil {
		// errorExit("Unable to get client: %v\n", err)
	}

	return Consumer{
		topic:         topic,
		consumerGroup: consumerGroup,
		offset:        offset,
		partitions:    partitions,
		markMessages:  markMessages,
		client:        client,
		messages:      make(chan *sarama.ConsumerMessage),
	}
}

type Consumer struct {
	topic         string
	brokers       []string
	consumerGroup string
	offset        int64
	partitions    []int32
	markMessages  bool
	client        sarama.Client
	messages      chan *sarama.ConsumerMessage
}

func (c *Consumer) Consume(ctx context.Context) chan *sarama.ConsumerMessage {
	if c.consumerGroup != "" {
		// TODO: return error
		c.consumeWithConsumerGroup(ctx)
		return c.messages
	}

	// TODO: return error
	c.consumeWithoutConsumerGroup(ctx)
	return c.messages
}

func (c *Consumer) consumeWithConsumerGroup(ctx context.Context) error {
	cg, err := sarama.NewConsumerGroupFromClient(c.consumerGroup, c.client)
	if err != nil {
		return err
	}

	err = cg.Consume(ctx, []string{c.topic}, c)
	return err
}

func (c *Consumer) consumeWithoutConsumerGroup(ctx context.Context) error {
	consumer, err := sarama.NewConsumerFromClient(c.client)
	if err != nil {
		return err
	}

	if len(c.partitions) == 0 {
		c.partitions, err = consumer.Partitions(c.topic)
		if err != nil {
			return err
		}
	}

	wg := &sync.WaitGroup{}
	for _, p := range c.partitions {
		wg.Add(1)
		go c.consume(wg, consumer, p, c.offset)
	}
	// wg.Wait()

	return nil
}

func (c *Consumer) consume(wg *sync.WaitGroup, consumer sarama.Consumer, partition int32, offset int64) error {
	defer wg.Done()

	cp, err := consumer.ConsumePartition(c.topic, partition, offset)
	if err != nil {
		return err
	}

	for msg := range cp.Messages() {
		c.messages <- msg
	}

	return nil
}

func getClientFromConfig(config *sarama.Config) (client sarama.Client) {
	return client
}

type offsets struct {
	newest, oldest int64
}

func getOffsets(client sarama.Client, topic string, partition int32) (offsets, error) {
	var off offsets
	var err error

	off.newest, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return off, err
	}

	off.oldest, err = client.GetOffset(topic, partition, sarama.OffsetOldest)
	return off, err
}
