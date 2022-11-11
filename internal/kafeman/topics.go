package kafeman

import (
	"context"
	"sync"
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

func (k *kafeman) DescribeTopic(ctx context.Context, topic string) {
	gl, err := k.GetGroupsList(ctx)
	if err != nil {
		return
	}

	memrs := make([]Member, 0)
	wg := &sync.WaitGroup{}

	for _, g := range gl {
		wg.Add(1)
		go func(gr string) {
			defer wg.Done()
			groups := k.DescribeGroup(ctx, gr)
			for _, m := range groups.Members {
				for _, a := range m.Assignments {
					if a.Topic == topic {
						memrs = append(memrs, m)
					}
				}
			}
		}(g)
	}

	wg.Wait()
	// adm := admin.NewAdmin(k.config)
	// adm.DescribeTopic(ctx, topic)
}
