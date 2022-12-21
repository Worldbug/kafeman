package kafeman

import (
	"context"
	"sync"
	"time"

	"github.com/worldbug/kafeman/internal/consumer"
	"github.com/worldbug/kafeman/internal/models"
)

/*
	1. Опционально падать при ошибке
	2. Не выводить ошибок если такие были
	3. Просто выводить ошибки но не падать
*/

type ConsumeCommand struct {
	Topic          string
	ConsumerGroup  string
	Partitions     []int32
	CommitMessages bool
	Offset         int64
	Follow         bool
	WithMeta       bool
	MessagesCount  int32
	FromTime       time.Time
	Decoder        Decoder
}

func (k *kafeman) Consume(ctx context.Context, cmd ConsumeCommand, decoder Decoder) (<-chan models.Message, error) {
	wg := &sync.WaitGroup{}

	c := consumer.NewSaramaConsuemr(
		k.config,
		cmd.ConsumerGroup,
		cmd.Topic,
		cmd.Partitions,
		cmd.Offset,
		cmd.MessagesCount,
		cmd.CommitMessages,
		cmd.Follow,
		cmd.FromTime,
	)

	messages, err := c.StartConsume(ctx)
	if err != nil {
		return nil, ErrNoTopicProvided
	}

	output := make(chan models.Message)
	go k.decodeMessages(messages, output, wg, decoder)
	wg.Wait()

	return output, nil
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
