package kafeman

import (
	"context"
	"fmt"
	"kafeman/internal/consumer"
	"kafeman/internal/handler"
	"kafeman/internal/models"
	"sync"

	"github.com/segmentio/kafka-go"
)

func (k *kafeman) Consume(ctx context.Context, cmd models.ConsumeCommand) {
	wg := &sync.WaitGroup{}

	h := handler.NewMessageHandler(wg, k.config, cmd)
	c := consumer.NewSaramaConsuemr(h, k.config, cmd)

	c.StartConsume(ctx)

	go h.Start()
	wg.Wait()
	h.Stop()
}

func toIntSlice(input []int32) []int {
	output := make([]int, len(input))
	for i, v := range input {
		output[i] = int(v)
	}

	return output
}

func (k *kafeman) partitions(partitions []int, topic string) []int {
	conn, err := kafka.Dial("tcp", k.config.GetCurrentCluster().Brokers[0])
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		fmt.Fprintln(k.errWriter, err)
		return []int{}
	}

	if len(partitions) != 0 {
		return partitions
	}

	for _, p := range parts {
		partitions = append(partitions, p.ID)
	}

	return partitions
}
