package kafeman

import (
	"context"
	"fmt"
	"sync"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/consumer"
	"github.com/worldbug/kafeman/internal/handler"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/serializers"

	"github.com/segmentio/kafka-go"
)

/*
	1. Опционально падать при ошибке
	2. Не выводить ошибок если такие были
	3. Просто выводить ошибки но не падать
*/

func (k *kafeman) Consume(ctx context.Context, cmd models.ConsumeCommand) error {
	wg := &sync.WaitGroup{}

	topic, ok := k.config.Topics[cmd.Topic]
	if !ok {
		return ErrNoTopicProvided
	}

	decoder, err := k.getDecoder(topic)
	if err != nil {
		return err
	}

	h := handler.NewMessageHandler(wg, k.config, cmd, decoder)
	c := consumer.NewSaramaConsuemr(h, k.config, cmd)

	c.StartConsume(ctx)

	go h.Start()
	wg.Wait()
	h.Stop()

	return nil
}

func (k *kafeman) getDecoder(topic config.Topic) (handler.Decoder, error) {
	if topic.ProtoType == "" || len(topic.ProtoPaths) == 0 {
		return serializers.NewRawSerializer(), nil
	}

	return serializers.NewProtobufSerializer(topic.ProtoPaths, topic.ProtoType)

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
