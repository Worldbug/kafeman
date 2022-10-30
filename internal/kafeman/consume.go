package kafeman

import (
	"context"
	"fmt"
	"kafeman/internal/proto"
	"sync"

	"github.com/segmentio/kafka-go"
)

func (k *kafeman) ConsumeV2(ctx context.Context, cmd ConsumeCommand) {
	if len(k.config.GetCurrentCluster().Brokers[0]) < 1 {
		return
	}

	if cmd.MessagesCount != 0 {
		cmd.limitedMessages = true
	}

	// TODO: ectract
	k.protoDecoder = *proto.NewProtobufDecoder(k.config.Topics[cmd.Topic].ProtoPaths)

	wg := &sync.WaitGroup{}
	if cmd.Follow {
		wg.Add(1)
	}

	topicPartitions := toIntSlice(cmd.Partitions)
	consumePartitions := k.partitions(topicPartitions, cmd.Topic)
	ch := make(chan Message, len(consumePartitions))

	for _, p := range consumePartitions {
		wg.Add(1)
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   k.config.GetCurrentCluster().Brokers,
			Topic:     cmd.Topic,
			Partition: p,
			GroupID:   cmd.ConsumerGroup,
		})

		reader.SetOffset(cmd.Offset)
		if cmd.FromTime.Unix() != 0 {
			reader.SetOffsetAt(ctx, cmd.FromTime)
		}

		go k.asyncConsume(ctx, reader, ch, cmd, wg)
	}

	wg.Wait()
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

func (k *kafeman) asyncConsume(ctx context.Context, reader *kafka.Reader, writer chan<- Message, cmd ConsumeCommand, wg *sync.WaitGroup) {
	defer wg.Done()
	remaring := cmd.MessagesCount

	for {
		select {
		case <-ctx.Done():
			break
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				fmt.Fprintln(k.errWriter, err)
				continue
			}
			k.messageHandler(fromKafkaMessage(msg), cmd.WithMeta)

			if reader.Lag() == 0 && !cmd.Follow {
				return
			}

			if cmd.CommitMessages {
				reader.CommitMessages(ctx, msg)
			}

			if cmd.limitedMessages {
				remaring--
			}

			if cmd.limitedMessages && remaring == 0 {
				return
			}

		}
	}
}

func fromKafkaMessage(msg kafka.Message) Message {
	headers := make(map[string]string)

	for _, h := range msg.Headers {
		// TODO: transform bytes
		headers[h.Key] = string(h.Value)
	}

	return Message{
		Headers:   headers,
		Timestamp: msg.Time,
		Offset:    msg.Offset,
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
	}
}

func (k *kafeman) messageHandler(messsage Message, withMeta bool) {
	if protoType := k.config.Topics[messsage.Topic].ProtoType; protoType != "" {
		messsage = k.handleProtoMessages(messsage, protoType)
	}

	k.printMessage(messsage, withMeta)
}
