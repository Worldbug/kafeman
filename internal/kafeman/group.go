package kafeman

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

func (k *kafeman) client() kafka.Client {
	return kafka.Client{
		Addr: kafka.TCP(k.config.GetCurrentCluster().Brokers...),
	}
}

func (k *kafeman) GetGroupsList(ctx context.Context) ([]string, error) {
	cli := k.client()
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

func (k *kafeman) DescribeGroups(ctx context.Context, groupList []string) ([]GroupInfo, error) {
	cli := k.client()

	batches := batchesFromSlice(groupList, 20)
	describe := make([]GroupInfo, 0, len(groupList))

	wg := &sync.WaitGroup{}
	m := &sync.Mutex{}
	for _, b := range batches {
		wg.Add(1)
		go func(batch []string, m *sync.Mutex, wg *sync.WaitGroup) {
			defer wg.Done()
			groups, err := cli.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
				GroupIDs: batch,
			})

			if err != nil {
				for _, name := range batch {
					describe = append(describe, GroupInfo{
						Name:      name,
						State:     "Empty",
						Consumers: 0,
					})

				}
				return
			}

			m.Lock()
			for _, g := range groups.Groups {
				describe = append(describe, GroupInfo{
					Name:      g.GroupID,
					State:     g.GroupState,
					Consumers: len(g.Members),
				})
			}
			m.Unlock()

		}(b, m, wg)
	}

	wg.Wait()

	return describe, nil
}

type GroupInfo struct {
	Name      string
	State     string
	Consumers int
}

func (k *kafeman) GetOffsetsForConsumer(ctx context.Context, group, topic string) (map[int]int64, error) {
	cli := k.client()
	offsets, err := cli.ConsumerOffsets(ctx, kafka.TopicAndGroup{
		Topic:   topic,
		GroupId: group,
	})

	return offsets, err
}

// TODO: refactor
func (k *kafeman) DescribeGroup(ctx context.Context, group string) Group {
	gd := NewGroup()
	cli := k.client()

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
			m := Member{
				Assignments: make([]Assignment, 0),
				Host:        member.ClientHost,
				ID:          member.MemberID,
			}

			for _, gmt := range member.MemberAssignments.Topics {
				if _, ok := groupTopis[gmt.Topic]; !ok {
					groupTopis[gmt.Topic] = make([]int, 0)
				}

				offsetsMap[gmt.Topic] = make(map[int]int64)
				wg.Add(1)
				go k.asyncGetLastOffset(ctx, wg, mu, offsetsMap, gmt.Topic, gmt.Partitions...)

				groupTopis[gmt.Topic] = append(groupTopis[gmt.Topic], gmt.Partitions...)
				m.Assignments = append(m.Assignments, Assignment{
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
			gd.Offsets[topic] = make([]Offset, 0)

			for _, ofp := range offsets {
				gd.Offsets[topic] = append(gd.Offsets[topic], Offset{
					Partition:      int32(ofp.Partition),
					Offset:         ofp.CommittedOffset,
					HightWatermark: offsetsMap[topic][ofp.Partition],
					Lag:            offsetsMap[topic][ofp.Partition] - ofp.CommittedOffset,
				})
			}
		}

	}

	return gd // k.GetOffsetsForTopic(ctx, gd)
}

func (k *kafeman) asyncGetLastOffset(ctx context.Context, wg *sync.WaitGroup, mu *sync.Mutex, offsetMap map[string]map[int]int64, topic string, parts ...int) {
	defer wg.Done()
	for p, o := range k.getLastOffset(ctx, topic, parts...) {
		mu.Lock()
		offsetMap[topic][p] = o
		mu.Unlock()
	}
}

func (k *kafeman) getLastOffset(ctx context.Context, topic string, partitions ...int) map[int]int64 {
	cli := k.client()
	result := make(map[int]int64)

	req := make(map[string][]kafka.OffsetRequest)
	req[topic] = make([]kafka.OffsetRequest, len(partitions))

	for i, p := range partitions {
		result[p] = -1
		req[topic][i].Partition = p
		req[topic][i].Timestamp = -1
	}

	offsets, err := cli.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: req,
	})
	if err != nil {
		return result
	}

	for _, p := range offsets.Topics[topic] {
		result[p.Partition] = p.LastOffset
	}

	return result
}

func (k *kafeman) DeleteGroup(group string) error {
	admin := k.getSaramaAdmin()
	return admin.DeleteConsumerGroup(group)
}

func (k *kafeman) CommitGroup(ctx context.Context, group, topic string, partitions []Offset) {
	cli := k.client()

	offsets := make(map[string][]kafka.OffsetCommit)
	offsets[topic] = make([]kafka.OffsetCommit, 0, len(partitions))

	for _, p := range partitions {
		offsets[topic] = append(offsets[topic], kafka.OffsetCommit{
			Partition: int(p.Partition),
			Offset:    p.Offset,
		})
	}

	cli.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		GroupID: group,
		Topics:  offsets,
	})
}