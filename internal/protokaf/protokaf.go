package protokaf

import (
	"io"

	"github.com/jcmturner/gokrb5/v8/config"
)

func NewProtokaf(
	config config.Config,
	outWriter io.Writer,
	errWriter io.Writer,
	inReader io.Reader,
) *Protokaf {
	return &Protokaf{
		config:    config,
		outWriter: outWriter,
		errWriter: errWriter,
		inReader:  inReader,
	}
}

type Protokaf struct {
	config config.Config

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader
}

func (pk *Protokaf) Consume(topic string) {

}
