package proto

import (
	"bytes"

	"github.com/golang/protobuf/jsonpb"
	pb "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

func NewProtobufDecoder(importPaths []string) *ProtobufDecoder {
	reg, err := NewDescriptorRegistry(importPaths, []string{})
	if err != nil {
		// TODO:
		panic(err)
	}

	return &ProtobufDecoder{
		protosRegistry: reg,
	}
}

type ProtobufDecoder struct {
	protosRegistry *DescriptorRegistry
}

func (pd *ProtobufDecoder) GetExample(protoType string) string {
	msg := pd.protosRegistry.MessageForType(protoType)

	m := jsonpb.Marshaler{
		EmitDefaults: true,
	}

	data, _ := m.MarshalToString(msg)
	return data
}

func (pd *ProtobufDecoder) DecodeProto(data []byte, protoType string) ([]byte, error) {
	messageContent, err := protoDecode(pd.protosRegistry, data, protoType)
	if err != nil {
		err = errors.Wrap(err, "Can`t decode proto content")
	}

	return messageContent, err

}

// proto to JSON
func protoDecode(reg *DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	err := dynamicMessage.Unmarshal(b)
	if err != nil {
		return nil, err
	}

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err = m.Marshal(&w, dynamicMessage)
	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func (pd *ProtobufDecoder) EncodeProto(raw []byte, protoType string) ([]byte, error) {
	msg := pd.protosRegistry.MessageForType(protoType)
	if msg == nil {
		return []byte{}, errors.New("Has no proto type")
	}

	err := msg.UnmarshalJSON(raw)
	if err != nil {
		return []byte{}, err
	}

	return pb.Marshal(msg)
}
