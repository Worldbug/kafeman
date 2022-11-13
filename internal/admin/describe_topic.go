package admin

import (
	"context"
)

/*
Это должно возвращать
консьюмеров лаг и оффсет
*/

func (a *Admin) DescribeTopic(ctx context.Context, topic string) {

	// adm := a.getSaramaAdmin()

	// // groups, _ := adm.ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32)
	// topicData, _ := adm.DescribeTopics([]string{topic})

	// parts := make(map[string][]int32)
	// parts[topic] = make([]int32, 0)

	// for _, tm := range topicData {
	// 	for _, part := range tm.Partitions {
	// 		parts[topic] = append(parts[topic], part.ID)
	// 	}
	// }

	// groupsMap, _ := adm.ListConsumerGroups()
	// groups := make([]string, 0, len(groupsMap))
	// for group := range groupsMap {
	// 	groups = append(groups, group)
	// }

	// wg := &sync.WaitGroup{}
	// for _, groups := range batchesFromSlice(groups, 50) {
	// 	wg.Add(1)
	// 	go func(groups []string) {
	// 		defer wg.Done()

	// 		for _, group := range groups {
	// 			resp, _ := adm.ListConsumerGroupOffsets(group, parts)
	// 		}

	// 	}(groups)
	// }
	// wg.Wait()

	// adm.ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32)

	// cgs, err := adm.ListConsumerGroups()

	// _ = cgs

	// md, _ := adm.DescribeTopics([]string{topic})
	// _ = md

	// pa, err := adm.ListPartitionReassignments(topic, []int32{0})
	// _ = pa

	///////////

	// cli := a.client()
	// ///
	// resp, err := cli.ListGroups(ctx, &kafka.ListGroupsRequest{})
	// if err != nil {
	// 	// TODO:
	// 	// return []string{}, err
	// }

	// groups := make(map[string]struct{}, len(resp.Groups))
	// for _, g := range resp.Groups {
	// 	groups[g.GroupID] = struct{}{}
	// }

	// groupsList := make([]string, 0, len(resp.Groups))
	// for _, g := range resp.Groups {
	// 	groupsList = append(groupsList, g.GroupID)
	// }

	// return groupsList, err
}
