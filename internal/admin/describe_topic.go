package admin

import (
	"context"

	"github.com/segmentio/kafka-go"
)

/*
Это должно возвращать
консьюмеров лаг и оффсет
*/

func (a *Admin) DescribeTopic(ctx context.Context, topic string) {
	cli := a.client()
	resp, err := cli.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		// TODO:
		// return []string{}, err
	}

	groups := make(map[string]struct{}, len(resp.Groups))
	for _, g := range resp.Groups {
		groups[g.GroupID] = struct{}{}
	}

	groupsList := make([]string, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groupsList = append(groupsList, g.GroupID)
	}

	// return groupsList, err
}
