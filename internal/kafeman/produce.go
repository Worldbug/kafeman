package kafeman

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/worldbug/kafeman/internal/producer"
	"github.com/worldbug/kafeman/internal/proto"
)

type ProduceCMD struct {
	Topic      string
	BufferSize int
}

func (k *kafeman) Produce(ctx context.Context, cmd ProduceCMD) {
	input := make(chan producer.Message, 1)

	producer := producer.NewProducer(k.config, input)
	go producer.Produce(cmd.Topic)

	k.marshall(cmd, input)
}

// TODO: доделать продюсерниг

func (k *kafeman) marshall(cmd ProduceCMD, input chan producer.Message) {
	k.protoDecoder = *proto.NewProtobufDecoder(k.config.Topics[cmd.Topic].ProtoPaths)

	rawInput := make(chan []byte, 1)
	go readLines(os.Stdin, cmd.BufferSize, rawInput)
	for raw := range rawInput {

		if protoType := k.config.Topics[cmd.Topic].ProtoType; protoType != "" {
			msg, err := k.protoDecoder.EncodeProto(raw, protoType)
			if err != nil {
				// TODO: stderror
				continue
			}

			input <- producer.Message{
				Key:   []byte{},
				Value: msg,
			}
		}
	}
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
		fmt.Fprintf(os.Stderr, "Can`t scan: %+v", err)
		os.Exit(1)
	}
}
