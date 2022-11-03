package handler

import (
	"io"
	"kafeman/internal/config"
	"kafeman/internal/models"
	"kafeman/internal/proto"
	"os"
	"sync"
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
	protoDecoder proto.ProtobufDecoder
	outWriter    io.Writer
	errWriter    io.Writer
}

func (mg *MessageHandler) Close() {
	mg.wg.Done()
}

func (mg *MessageHandler) Start() {
	mg.closeWG.Add(1)
	defer mg.closeWG.Done()
	for m := range mg.messages {
		mg.handle(m)
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
	if protoType := mg.config.Topics[message.Topic].ProtoType; protoType != "" {
		message = mg.handleProtoMessages(message, protoType)
	}

	mg.printMessage(message, mg.cmd.WithMeta)
}

func (mg *MessageHandler) Stop() {
	if mg.messages != nil {
		close(mg.messages)
	}
	mg.closeWG.Wait()
}