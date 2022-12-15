package producer

import (
	"fmt"
	"kafeman/internal/config"

	"github.com/Shopify/sarama"
)

type Message struct {
	Key   []byte
	Value []byte
}

func NewProducer(
	config config.Config,
	input <-chan Message,
) *Producer {
	return &Producer{
		config: config,
		input:  input,
	}
}

type Producer struct {
	config config.Config
	input  <-chan Message
}

// TODO: new sarama from conf

func (p *Producer) Produce(topic string) {
	addrs := p.config.GetCurrentCluster().Brokers

	producer, err := sarama.NewSyncProducer(addrs, p.getSaramaConfig())
	if err != nil {
		return
	}

	for msg := range p.input {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			// Headers:   []sarama.RecordHeader{},
			// Partition: 0,
			// Timestamp: time.Now(),
			Key:   sarama.ByteEncoder(msg.Key),
			Value: sarama.ByteEncoder(msg.Value),
		})

		fmt.Println(err)
	}
}

func (p *Producer) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true

	return saramaConfig
}
