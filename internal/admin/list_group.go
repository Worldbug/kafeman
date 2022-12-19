package admin

import (
	"context"

	"github.com/segmentio/kafka-go"
)

func (a *Admin) GetGroupsList(ctx context.Context) ([]string, error) {
	cli := a.client()
	resp, err := cli.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return []string{}, err
	}

	groups := make(map[string]struct{}, len(resp.Groups))
	for _, g := range resp.Groups {
		groups[g.GroupID] = struct{}{}
	}

	groupsList := make([]string, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groupsList = append(groupsList, g.GroupID)
	}

	return groupsList, err
}
