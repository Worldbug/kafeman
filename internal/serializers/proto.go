package serializers

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	pb "github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
)

func NewProtobufSerializer(importPaths []string, protoType string) (*ProtobufSerializer, error) {
	reg, err := NewDescriptorRegistry(importPaths, []string{})
	if err != nil {
		return nil, err
	}

	return &ProtobufSerializer{
		protosRegistry: reg,
		protoType:      protoType,
	}, err
}

type ProtobufSerializer struct {
	protosRegistry *DescriptorRegistry
	protoType      string
}

func (ps *ProtobufSerializer) GetExample(protoType string) string {
	msg := ps.protosRegistry.MessageForType(protoType)

	m := jsonpb.Marshaler{
		EmitDefaults: true,
	}

	data, _ := m.MarshalToString(msg)
	return data
}

func (ps *ProtobufSerializer) Decode(data []byte) ([]byte, error) {
	messageContent, err := protoDecode(ps.protosRegistry, data, ps.protoType)
	if err != nil {
		err = errors.Wrap(err, "Can`t decode proto content")
	}

	return messageContent, err
}

func (ps *ProtobufSerializer) Encode(raw []byte) ([]byte, error) {
	msg := ps.protosRegistry.MessageForType(ps.protoType)
	if msg == nil {
		return []byte{}, errors.New("Has no proto type")
	}

	err := msg.UnmarshalJSON(raw)
	if err != nil {
		return []byte{}, err
	}

	return pb.Marshal(msg)
}

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

type DescriptorRegistry struct {
	descriptors []*desc.FileDescriptor
}

func NewDescriptorRegistry(importPaths []string, exclusions []string) (*DescriptorRegistry, error) {
	// TODO: os depend abs path
	for i, path := range importPaths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}

		importPaths[i] = absPath
	}

	p := &protoparse.Parser{
		ImportPaths: importPaths,
	}

	var protoFiles []string

	for _, importPath := range importPaths {
		err := filepath.Walk(importPath, func(path string, info os.FileInfo, err error) error {
			if info != nil && !info.IsDir() && strings.HasSuffix(path, ".proto") {
				protoFiles = append(protoFiles, path)
			}

			return nil
		})
		if err != nil {
			return nil, err
		}

	}

	resolved, err := protoparse.ResolveFilenames(importPaths, protoFiles...)
	if err != nil {
		return nil, err
	}

	var deduped []string
	for _, i := range resolved {

		var exclusionFound bool
		for _, exclusion := range exclusions {
			if strings.HasPrefix(i, exclusion) {
				exclusionFound = true
				break
			}
		}

		if !exclusionFound {
			deduped = append(deduped, i)
		}
	}

	descs, err := p.ParseFiles(deduped...)
	if err != nil {
		return nil, err
	}

	return &DescriptorRegistry{descriptors: descs}, nil
}

func (d *DescriptorRegistry) MessageForType(_type string) *dynamic.Message {
	for _, descriptor := range d.descriptors {
		if messageDescriptor := descriptor.FindMessage(_type); messageDescriptor != nil {
			return dynamic.NewMessage(messageDescriptor)
		}
	}
	return nil
}
