package admin

import (
	"context"

	"github.com/worldbug/kafeman/internal/logger"
)

func (a *Admin) DeleteTopic(ctx context.Context, topic string) error {
	adm := a.getSaramaAdmin()
	err := adm.DeleteTopic(topic)
	if err != nil {
		logger.Errorf("Delete topic [%s] err: %+w", topic, err)
	}

	return err
}
