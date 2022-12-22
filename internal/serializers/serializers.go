package serializers

var SupportedSerializers = []string{
	"raw", "protobuf", "avro", "msgpack", "base64",
}

// Hold implementations for kafeman.Decoder/kafeman.Encoder

func NewRawSerializer() *RawSerializer {
	return &RawSerializer{}
}

type RawSerializer struct{}

func (re *RawSerializer) Encode(input []byte) ([]byte, error) {
	return input, nil
}

func (re *RawSerializer) Decode(input []byte) ([]byte, error) {
	return input, nil
}
