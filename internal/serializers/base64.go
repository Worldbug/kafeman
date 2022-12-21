package serializers

import (
	"encoding/base64"
)

func NewBase64Serializer() *Base64Serializer {
	return &Base64Serializer{}
}

type Base64Serializer struct{}

func (bs *Base64Serializer) Encode(input []byte) ([]byte, error) {
	encoded := base64.StdEncoding.EncodeToString(input)
	return []byte(encoded), nil
}

func (bs *Base64Serializer) Decode(input []byte) ([]byte, error) {
	return base64.StdEncoding.DecodeString(string(input))
}
