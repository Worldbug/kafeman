package consumer

import (
	"github.com/Shopify/sarama"
)

type KafkaConsumerGroup struct {
	commitConsumedMessages bool
}

func (cg *KafkaConsumerGroup) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (cg *KafkaConsumerGroup) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (cg *KafkaConsumerGroup) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		if cg.commitConsumedMessages {
			s.MarkMessage(msg, "")
		}
	}
	return nil
}
