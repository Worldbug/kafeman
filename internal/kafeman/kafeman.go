package kafeman

import (
	"errors"
	"io"
	"os"

	"github.com/worldbug/kafeman/internal/config"
)

var ErrNoTopicProvided = errors.New("No topic provided")

// Encoder declare how to encode message
type Encoder interface {
	Encode([]byte) ([]byte, error)
}

// Encoder declare how to encode message
type Decoder interface {
	Decode([]byte) ([]byte, error)
}

func Newkafeman(config *config.Configuration) *kafeman {
	return &kafeman{
		config:    config,
		outWriter: os.Stdout,
		errWriter: os.Stderr,
	}
}

type kafeman struct {
	config *config.Configuration

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader
}
