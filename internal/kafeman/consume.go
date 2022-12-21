package kafeman

import (
	"context"
	"sync"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/consumer"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/serializers"
)

/*
	1. Опционально падать при ошибке
	2. Не выводить ошибок если такие были
	3. Просто выводить ошибки но не падать
*/

func (k *kafeman) Consume(ctx context.Context, cmd models.ConsumeCommand) (<-chan models.Message, error) {
	wg := &sync.WaitGroup{}

	topic, ok := k.config.Topics[cmd.Topic]
	if !ok {
		return nil, ErrNoTopicProvided
	}

	decoder, err := k.getDecoder(topic)
	if err != nil {
		return nil, ErrNoTopicProvided
	}

	c := consumer.NewSaramaConsuemr(k.config, cmd)

	messages, err := c.StartConsume(ctx)
	if err != nil {
		return nil, ErrNoTopicProvided
	}

	output := make(chan models.Message)
	go k.decodeMessages(messages, output, wg, decoder)
	wg.Wait()

	return output, nil
}

func (k *kafeman) getDecoder(topic config.Topic) (Decoder, error) {
	if topic.ProtoType == "" || len(topic.ProtoPaths) == 0 {
		return serializers.NewRawSerializer(), nil
	}

	return serializers.NewProtobufSerializer(topic.ProtoPaths, topic.ProtoType)
}

func (k *kafeman) decodeMessages(
	input <-chan models.Message, output chan<- models.Message,
	wg *sync.WaitGroup, decoder Decoder) {
	wg.Add(1)
	defer wg.Done()
	defer close(output)

	for {
		select {
		case message, ok := <-input:
			if !ok {
				return
			}

			// Потом это станет valueDecoder
			value, err := decoder.Decode(message.Value)
			if err != nil {
				// TODO: error
			}

			message.Value = value
			output <- message
		}
	}
}
