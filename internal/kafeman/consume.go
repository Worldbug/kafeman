package kafeman

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/worldbug/kafeman/internal/consumer"
	"github.com/worldbug/kafeman/internal/logger"
	"github.com/worldbug/kafeman/internal/models"
)

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
	ToTime         time.Time
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
		cmd.ToTime,
	)

	messages, err := c.StartConsume(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Start consume error:")
	}

	output := make(chan models.Message)
	go k.decodeMessages(ctx, messages, output, wg, decoder)
	wg.Wait()

	return output, nil
}

func (k *kafeman) decodeMessages(
	ctx context.Context,
	input <-chan models.Message, output chan<- models.Message,
	wg *sync.WaitGroup, decoder Decoder) {
	wg.Add(1)
	defer wg.Done()
	defer close(output)

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-input:
			if !ok {
				return
			}

			// Потом это станет valueDecoder
			value, err := decoder.Decode(message.Value)
			if err != nil {
				logger.Fatalf("%+v\n", err)
			}

			message.Value = value
			output <- message
		}
	}
}
