package consumer

import (
	"context"

	"github.com/Shopify/sarama"
)

// TODO: map partitions on offset if offset not empty
func NewConsumer(topic, consumerGroup string, partitions []int, markMessages bool, offset int64, brokers []string) Consumer {
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
	partitions    []int
	markMessages  bool
	client        sarama.Client
	messages      chan *sarama.ConsumerMessage
}

func (c *Consumer) Consume(ctx context.Context) chan *sarama.ConsumerMessage {
	if c.consumerGroup != "" {
		c.consumeWithConsumerGroup(ctx)
		return c.messages
	}

	c.consume(ctx)
	return c.messages
}

func (c *Consumer) consume(ctx context.Context) {
	cg, err := sarama.NewConsumerGroupFromClient(c.consumerGroup, c.client)
	if err != nil {
		return
		//errorExit("Failed to create consumer group: %v", err)
	}

	err = cg.Consume(ctx, []string{c.topic}, c)
	if err != nil {
		return
		///// errorExit("Error on consume: %v", err)
	}
}

func (c *Consumer) consumeWithConsumerGroup(ctx context.Context) {
	// client := sarama.Client{

	// consumer, err := sarama.NewConsumerFromClient(client)
	// if err != nil {
	// 	// errorExit("Unable to create consumer from client: %v\n", err)
	// }
}

func (c *Consumer) getClient()

func getClientFromConfig(config *sarama.Config) (client sarama.Client) {
	return client
}
