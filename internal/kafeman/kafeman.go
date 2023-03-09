package kafeman

import (
	"context"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/models"
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

func Newkafeman(
	config *config.Config,
) *kafeman {

	return &kafeman{
		config:    config,
		outWriter: os.Stdout,
		errWriter: os.Stderr,
	}
}

type kafeman struct {
	config *config.Config

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader
}

func (k *kafeman) ListTopics(ctx context.Context) ([]models.Topic, error) {
	adm := admin.NewAdmin(k.config)

	topics, err := adm.ListTopics(ctx)
	if err != nil {
		return []models.Topic{}, err
	}

	sort.Slice(topics, func(i int, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	return topics, nil
}
