package config

type Protobuf struct {
	ProtoPaths []string          `yaml:"proto_paths"`
	Topics     map[string]string `yaml:"topics"`
}
