package serializers

func NewBase64Serializer() *Base64Serializer {
	return &Base64Serializer{}
}

type Base64Serializer struct{}

func (bs *Base64Serializer) Encode(input []byte) ([]byte, error) {
	return input, nil
}

func (bs *Base64Serializer) Decode(input []byte) ([]byte, error) {
	return input, nil
}
