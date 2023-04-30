package config

type Encoding string

const (
	MSGPack  Encoding = "msgpack"
	Protobuf Encoding = "protobuf"
	Avro     Encoding = "avro"
	RAW      Encoding = "raw"
	Base64   Encoding = "base64"
)

type Topic struct {
	Name          string   `mapstructure:"name"`
	Encoding      Encoding `mapstructure:"encoding,omitempty"`
	ProtoType     string   `mapstructure:"proto_type,omitempty"`
	ProtoPaths    []string `mapstructure:"proto_paths,omitempty"`
	AvroSchemaURL string   `mapstructure:"avro_schema_url,omitempty"`
	AvroSchemaID  int      `mapstructure:"avro_schema_id,omitempty"`
}

// func (t Topic) Marshall() []byte {
// 	topicYAML, err := yaml.Marshal(t)
// 	if err != nil {
// 		return []byte{}
// 	}
// 	return topicYAML
// }
//
// func (t *Topic) Value() (driver.Value, error) {
// 	return yaml.Marshal(t)
// }
//
