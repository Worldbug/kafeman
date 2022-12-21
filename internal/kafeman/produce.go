package kafeman

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/producer"
	"github.com/worldbug/kafeman/internal/serializers"
)

type ProduceCMD struct {
	Topic string
	// One of ......
	Encoder    string
	BufferSize int
	Input      io.Reader
	Output     io.Writer
}

func (k *kafeman) Produce(ctx context.Context, cmd ProduceCMD) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	input := make(chan producer.Message, 1)
	topic, ok := k.config.Topics[cmd.Topic]
	if !ok {
		return ErrNoTopicProvided
	}

	encoder, err := k.GetEncoder(topic)
	if err != nil {
		return err
	}

	producer := producer.NewProducer(k.config, input)
	go producer.Produce(cmd.Topic, wg)

	k.marshall(cmd, encoder, input)
	close(input)

	return nil
}

func (k *kafeman) GetEncoder(topic config.Topic) (Encoder, error) {
	if topic.ProtoType == "" || len(topic.ProtoPaths) == 0 {
		return serializers.NewRawSerializer(), nil
	}

	return serializers.NewProtobufSerializer(topic.ProtoPaths, topic.ProtoType)
}

func (k *kafeman) marshall(cmd ProduceCMD, encoder Encoder, input chan producer.Message) {
	rawInput := readLinesToChan(cmd.Input, cmd.BufferSize)
	for raw := range rawInput {

		value, err := encoder.Encode(raw)
		if err != nil {
			// TODO: error
		}

		input <- producer.Message{
			Key:   []byte{},
			Value: value,
		}
	}
}

func readLinesToChan(reader io.Reader, bufferSize int) <-chan []byte {
	out := make(chan []byte)
	go readLines(reader, bufferSize, out)

	return out
}

func readLines(reader io.Reader, bufferSize int, out chan []byte) {
	scanner := bufio.NewScanner(reader)
	if bufferSize > 0 {
		scanner.Buffer(make([]byte, bufferSize), bufferSize)
	}
	for scanner.Scan() {
		out <- scanner.Bytes()
	}
	close(out)

	if err := scanner.Err(); err != nil {
		// TODO: опционально ничего не писать
		fmt.Fprintf(os.Stderr, "Can`t scan: %+v", err)
		os.Exit(1)
	}
}
