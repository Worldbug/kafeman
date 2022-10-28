package config

import (
	"database/sql/driver"

	"gopkg.in/yaml.v2"
)

type Topic struct {
	ProtoType  string   `yaml:"proto_type"`
	ProtoPaths []string `yaml:"proto_paths"`
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
