package handler

import (
	"fmt"
	"kafeman/internal/models"
	"sync"
)

func NewMessageHandler(wg *sync.WaitGroup) *MessageHandler {
	return &MessageHandler{
		wg:      wg,
		closeWG: &sync.WaitGroup{},
	}
}

type MessageHandler struct {
	messages chan models.Message
	wg       *sync.WaitGroup
	closeWG  *sync.WaitGroup
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
	fmt.Println(message)
}

func (mg *MessageHandler) Stop() {
	if mg.messages != nil {
		close(mg.messages)
	}
	mg.closeWG.Wait()
}
