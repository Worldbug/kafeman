package config

import (
	"database/sql/driver"

	"gopkg.in/yaml.v2"
)

type Encoding string

const (
	MSGPack  Encoding = "msgpack"
	Protobuf Encoding = "protobuf"
	Avro     Encoding = "avro"
	RAW      Encoding = "raw"
	Base64   Encoding = "base64"
)

type Topic struct {
	Encoding      Encoding `yaml:"encoding"`
	ProtoType     string   `yaml:"proto_type"`
	ProtoPaths    []string `yaml:"proto_paths"`
	AvroSchemaURL string   `yaml:"avro_schema_url"`
	AvroSchemaID  int      `yaml:"avro_schema_id"`
}

func (t Topic) Marshall() []byte {
	pointJSONB, err := yaml.Marshal(t)
	if err != nil {
		return []byte{}
	}
	return pointJSONB
}

func (t *Topic) Value() (driver.Value, error) {
	return yaml.Marshal(t)
}
