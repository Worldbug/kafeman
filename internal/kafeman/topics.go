package kafeman

import (
	"context"
)

func (k *kafeman) GetTopicInfo(ctx context.Context, topic string) Topic {
	topics := k.ListTopics(ctx)

	for _, t := range topics {
		if t.Name == topic {
			return t
		}
	}

	return Topic{}
}
