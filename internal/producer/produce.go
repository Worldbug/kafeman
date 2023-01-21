package producer

import (
	"sync"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/logger"
	"github.com/worldbug/kafeman/internal/sarama_config"
	"github.com/worldbug/kafeman/internal/utils"

	"github.com/Shopify/sarama"
)

type Message struct {
	Key   []byte
	Value []byte
}

func NewProducer(
	config config.Config,
	partitioner string,
	partition int32,
	input <-chan Message,
) (*Producer, error) {
	addrs := config.GetCurrentCluster().Brokers
	saramaConfig, err := getSaramaConfig(
		config, partitioner, partition)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(addrs, saramaConfig)
	if err != nil {
		return &Producer{}, nil
	}

	return &Producer{
		config:      config,
		input:       input,
		producer:    producer,
		partitioner: partitioner,
		partition:   partition,
	}, nil
}

type Producer struct {
	config   config.Config
	input    <-chan Message
	producer sarama.SyncProducer

	partitioner string
	partition   int32
}

func (p *Producer) Produce(topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	for msg := range p.input {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(msg.Key),
			Value: sarama.ByteEncoder(msg.Value),
			// TODO:
			// Headers:   []sarama.RecordHeader{},
			// Timestamp: time.Now(),
		}

		if p.partition != -1 {
			msg.Partition = p.partition
		}

		partition, offset, err := p.producer.SendMessage(msg)
		if err != nil {
			logger.Fatalf("%+v\n", err)
			continue
		}

		logger.Infof("partition: %d\toffset: %d\n", partition, offset)
	}
}

func getSaramaConfig(
	conf config.Config,
	partitioner string,
	partition int32,
) (*sarama.Config, error) {

	saramaConfig, err := sarama_config.GetSaramaFromConfig(conf)
	if err != nil {
		return nil, err
	}

	switch partitioner {
	case "jvm":
		saramaConfig.Producer.Partitioner = utils.NewJVMCompatiblePartitioner
	case "rand":
		saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	case "rr":
		saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	case "hash":
		saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
	default:
		saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
	}

	if partition != -1 {
		saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
	}

	return saramaConfig, nil
}
