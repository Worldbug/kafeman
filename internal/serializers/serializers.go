package serializers

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