package kafeman

import (
	"context"
	"sync"

	"github.com/worldbug/kafeman/internal/consumer"
	"github.com/worldbug/kafeman/internal/models"
)

type DumpCommand struct {
	ConsumeCommand
}

func (k *kafeman) Dump(ctx context.Context, cmd DumpCommand, decoder Decoder) (<-chan models.Message, error) {
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
	go k.decodeMessages(ctx, messages, output, wg, decoder)
	wg.Wait()

	return output, nil
}
