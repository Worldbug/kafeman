package kafeman

import (
	"context"
	"sync"

	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/utils"

	"github.com/segmentio/kafka-go"
)

func (k *kafeman) client() kafka.Client {
	return kafka.Client{
		Addr: kafka.TCP(k.config.GetCurrentCluster().Brokers...),
	}
}

func (k *kafeman) GetGroupsList(ctx context.Context) ([]string, error) {
	return admin.NewAdmin(k.config).GetGroupsList(ctx)
}

func (k *kafeman) DescribeGroups(ctx context.Context, groupList []string) ([]GroupInfo, error) {
	cli := k.client()

	batches := utils.BatchesFromSlice(groupList, 20)
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

func (k *kafeman) DescribeGroup(ctx context.Context, group string) models.Group {
	return admin.NewAdmin(k.config).DescribeGroup(ctx, group)
}

func (k *kafeman) DeleteGroup(group string) error {
	return admin.NewAdmin(k.config).DeleteGroup(group)
}

func (k *kafeman) SetGroupOffset(ctx context.Context, group, topic string, partitions []models.Offset) {
	for _, p := range partitions {
		r := kafka.NewReader(kafka.ReaderConfig{
			GroupID:     group,
			Brokers:     k.config.GetCurrentCluster().Brokers,
			Topic:       topic,
			StartOffset: kafka.FirstOffset,
			Partition:   int(p.Partition),
		})

		msg, _ := r.FetchMessage(ctx)

		msg.Offset = p.Offset
		r.CommitMessages(ctx, msg)
		r.Close()
	}

	return
}
