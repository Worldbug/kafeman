package kafeman

import (
	"context"
	"io"
	"os"
	"sort"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/config"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/proto"
)

func Newkafeman(
	config config.Config,
) *kafeman {

	return &kafeman{
		config:    config,
		outWriter: os.Stdout,
		errWriter: os.Stderr,
	}
}

type kafeman struct {
	config config.Config

	outWriter io.Writer
	errWriter io.Writer
	inReader  io.Reader

	protoDecoder proto.ProtobufDecoder
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
