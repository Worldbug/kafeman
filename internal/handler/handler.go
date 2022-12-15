package handler

import (
	"io"
	"os"
	"sync"

	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/proto"
)

func NewMessageHandler(wg *sync.WaitGroup, config config.Config, cmd models.ConsumeCommand) *MessageHandler {
	return &MessageHandler{
		wg:        wg,
		closeWG:   &sync.WaitGroup{},
		config:    config,
		cmd:       cmd,
		outWriter: os.Stdout,
		errWriter: os.Stderr,
	}
}

type MessageHandler struct {
	config       config.Config
	cmd          models.ConsumeCommand
	messages     chan models.Message
	wg           *sync.WaitGroup
	closeWG      *sync.WaitGroup
	protoDecoder *proto.ProtobufDecoder
	outWriter    io.Writer
	errWriter    io.Writer

	currentTopic string
}

func (mg *MessageHandler) Close() {
	mg.wg.Done()
}

func (mg *MessageHandler) Start() {
	mg.closeWG.Add(1)
	defer mg.closeWG.Done()
	for {
		select {
		case m, ok := <-mg.messages:
			if !ok {
				return
			}

			mg.handle(m)
		}
	}
}

func (mg *MessageHandler) GetInputChan() chan models.Message {
	return mg.messages
}

func (mg *MessageHandler) InitInput(inputThreads int) {
	mg.wg.Add(inputThreads)
	mg.messages = make(chan models.Message, inputThreads)
}

func (mg *MessageHandler) Handle(message models.Message) {
	mg.messages <- message
}

func (mg *MessageHandler) handle(message models.Message) {
	mg.setProtoDecoder(message)

	if protoType := mg.config.Topics[message.Topic].ProtoType; protoType != "" {
		message = mg.handleProtoMessages(message, protoType)
	}

	mg.printMessage(message, mg.cmd.WithMeta)
}

func (mg *MessageHandler) setProtoDecoder(message models.Message) {
	if mg.currentTopic != message.Topic {
		mg.protoDecoder = proto.NewProtobufDecoder(mg.config.Topics[message.Topic].ProtoPaths)
		mg.currentTopic = message.Topic
	}
}

func (mg *MessageHandler) Stop() {
	if mg.messages != nil {
		close(mg.messages)
	}
	mg.closeWG.Wait()
}
