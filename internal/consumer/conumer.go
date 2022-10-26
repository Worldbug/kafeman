package consumer

import (
	"sync"

	"github.com/Shopify/sarama"
)

// MessageHandler ...
type MessageHandler func(*sarama.ConsumerMessage, *sync.Mutex)

// KafkaConsumerGroup ...
type KafkaConsumerGroup struct {
	commitConsumedMessages bool
	messageHandler         MessageHandler
}

func (cg *KafkaConsumerGroup) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (cg *KafkaConsumerGroup) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (cg *KafkaConsumerGroup) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	for msg := range claim.Messages() {
		cg.messageHandler(msg, &mu)
		if cg.commitConsumedMessages {
			s.MarkMessage(msg, "")
		}
	}
	return nil
}
