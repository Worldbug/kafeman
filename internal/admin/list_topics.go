package admin

import (
	"context"

	"github.com/worldbug/kafeman/internal/models"
)

func (a *Admin) ListTopics(ctx context.Context) ([]models.Topic, error) {
	conn, err := a.conn()
	if err != nil {
		return []models.Topic{}, err
	}

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return []models.Topic{}, err
	}

	topics := map[string]models.Topic{}

	for _, p := range partitions {
		if info, ok := topics[p.Topic]; ok {
			info.Partitions++
			info.Replicas += len(p.Replicas)
			topics[p.Topic] = info
			continue
		}

		topics[p.Topic] = models.Topic{
			Name:       p.Topic,
			Partitions: 1,
			Replicas:   len(p.Replicas),
		}
	}

	sortedTopics := make([]models.Topic, 0, len(topics))
	for _, topic := range topics {
		sortedTopics = append(sortedTopics, topic)
	}

	return sortedTopics, err
}
