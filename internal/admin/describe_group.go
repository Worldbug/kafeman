package admin

import (
	"context"
	"fmt"
	"sync"

	"github.com/worldbug/kafeman/internal/models"

	"github.com/segmentio/kafka-go"
)

func (a *Admin) DescribeGroup(ctx context.Context, group string) models.Group {
	gd := models.NewGroup()
	cli := a.client()

	groupsDescribe, err := cli.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		GroupIDs: []string{group},
	})
	if err != nil {
		return gd
	}

	offsetsMap := make(map[string]map[int]int64)
	mu := &sync.Mutex{}
	groupTopis := make(map[string][]int)

	wg := &sync.WaitGroup{}

	for _, group := range groupsDescribe.Groups {
		gd.GroupID = group.GroupID
		gd.State = group.GroupState

		for _, member := range group.Members {
			m := models.Member{
				Assignments: make([]models.Assignment, 0),
				Host:        member.ClientHost,
				ID:          member.MemberID,
			}

			for _, gmt := range member.MemberAssignments.Topics {
				if _, ok := groupTopis[gmt.Topic]; !ok {
					groupTopis[gmt.Topic] = make([]int, 0)
				}

				offsetsMap[gmt.Topic] = make(map[int]int64)
				wg.Add(1)
				go a.asyncGetLastOffset(ctx, wg, mu, offsetsMap, gmt.Topic, gmt.Partitions...)

				groupTopis[gmt.Topic] = append(groupTopis[gmt.Topic], gmt.Partitions...)
				m.Assignments = append(m.Assignments, models.Assignment{
					Topic:      gmt.Topic,
					Partitions: gmt.Partitions,
				})
			}

			gd.Members = append(gd.Members, m)
		}

		offsets, err := cli.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			GroupID: gd.GroupID,
			Topics:  groupTopis,
		})
		if err != nil {
			continue
		}

		wg.Wait()
		for topic, offsets := range offsets.Topics {
			gd.Offsets[topic] = make([]models.Offset, 0)

			for _, ofp := range offsets {
				gd.Offsets[topic] = append(gd.Offsets[topic], models.Offset{
					Partition:      int32(ofp.Partition),
					Offset:         ofp.CommittedOffset,
					HightWatermark: offsetsMap[topic][ofp.Partition],
					Lag:            offsetsMap[topic][ofp.Partition] - ofp.CommittedOffset,
				})
			}
		}

	}

	return gd
}

func (a *Admin) fetchLastOffset(ctx context.Context, topic string, partition int) models.Offset {
	cli := a.client()
	resp, err := cli.Fetch(ctx, &kafka.FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    -1,
	})
	if err != nil {
		fmt.Println(err)
		return models.Offset{}
	}

	return models.Offset{
		Partition:      int32(resp.Partition),
		HightWatermark: resp.HighWatermark,
	}
}
