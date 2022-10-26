package proto

import (
	"bytes"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/pkg/errors"
)

type ProtobufDecoder struct {
	protosRegistry *DescriptorRegistry
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
