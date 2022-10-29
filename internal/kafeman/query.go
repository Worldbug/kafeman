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

func (k *kafeman) GetOffsetByTimestamp(ctx context.Context) {
	// if len(k.config.GetCurrentCluster().Brokers[0]) < 1 {
	// 	return
	// }

	// cli := kafka.Client{}

	// cli.ConsumerOffsets(ctx, kafka.TopicAndGroup{
	// 	Topic: string,
	// })

	// conn, err := kafka.Dial("tcp", k.config.GetCurrentCluster().Brokers[0])
	// if err != nil {
	// 	panic(err.Error())
	// }
	// defer conn.Close()

	// oo, ee := conn.ReadOffset(time.Now())
	// fmt.Println(oo, ee)

	// for _, t := range k.ListTopics(ctx) {
	// 	if t.Name != "processing-state-events" {
	// 		continue
	// 	}

	// 	for i := t.Partitions - 1; i >= 0; i-- {
	// 		conn, err := kafka.Dial("tcp", k.config.GetCurrentCluster().Brokers[0])
	// 		if err != nil {
	// 			panic(err.Error())
	// 		}
	// 		defer conn.Close()
	// 		c := kafka.NewConnWith(conn, kafka.ConnConfig{
	// 			Topic:     "processing-state-events",
	// 			Partition: i,
	// 		})

	// 		o, e := c.ReadOffset(time.Now().Add(-time.Hour))

	// 		fmt.Printf("Offset: %v Partition: %v Err: %v\n", o, i, e)
	// 	}

	// }
}
