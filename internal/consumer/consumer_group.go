package consumer

import (
	"github.com/Shopify/sarama"
)

func (c *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if c.markMessages {
			c.messages <- msg
			s.MarkMessage(msg, "")
		}
	}

	return nil
}
