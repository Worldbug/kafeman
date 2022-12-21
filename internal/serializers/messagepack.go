package serializers

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

func NewMessagePackSerializer() *MessagePackSerializer {
	return &MessagePackSerializer{}
}

type MessagePackSerializer struct{}

func (mp *MessagePackSerializer) Encode(input []byte) ([]byte, error) {
	var obj interface{}
	err := json.Unmarshal(input, &obj)
	if err != nil {
		return nil, err
	}

	return msgpack.Marshal(obj)
}

func (mp *MessagePackSerializer) Decode(input []byte) ([]byte, error) {
	var obj interface{}
	err := msgpack.Unmarshal(input, &obj)
	if err != nil {
		return nil, err
	}

	return json.Marshal(obj)
}
