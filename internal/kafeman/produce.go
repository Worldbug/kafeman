package kafeman

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/worldbug/kafeman/internal/producer"
	"github.com/worldbug/kafeman/internal/serializers"
)

type ProduceCMD struct {
	Topic string
	// One of raw,proto,avro,msgpack
	// Encoder    string
	BufferSize int
	Input      io.Reader
	Output     io.Writer
}

func (k *kafeman) Produce(ctx context.Context, cmd ProduceCMD) error {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	input := make(chan producer.Message, 1)

	encoder, err := k.getEncoder(cmd.Topic)
	if err != nil {
		return err
	}

	producer := producer.NewProducer(k.config, input)
	go producer.Produce(cmd.Topic, wg)

	k.encodeMessages(cmd, encoder, input)
	close(input)

	return nil
}

func (k *kafeman) getEncoder(topic string) (Encoder, error) {
	topicConfig, ok := k.config.Topics[topic]
	if !ok {
		return serializers.NewRawSerializer(), nil
	}

	if topicConfig.ProtoType == "" || len(topicConfig.ProtoPaths) == 0 {
		return serializers.NewRawSerializer(), nil
	}

	return serializers.NewProtobufSerializer(topicConfig.ProtoPaths, topicConfig.ProtoType)
}

func (k *kafeman) encodeMessages(cmd ProduceCMD, encoder Encoder, input chan producer.Message) {
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
