package kafeman

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"github.com/worldbug/kafeman/internal/admin"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/sarama_config"
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

func (k *kafeman) SetGroupOffset(ctx context.Context, group, topic string, offsets []models.Offset) error {
	config, err := sarama_config.GetSaramaFromConfig(k.config)
	if err != nil {
		return errors.Wrap(err, "Cant create sarama config")
	}

	client, err := sarama.NewClient(k.config.GetCurrentCluster().Brokers, config)
	if err != nil {
		return errors.Wrapf(err, "Error create client for group %s", group)
	}

	groupInfo := k.DescribeGroup(ctx, group)
	_, ok := groupInfo.Offsets[topic]
	if ok {
		return errors.New("Group has active consumers on this topic")
	}

	cg, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		return errors.Wrap(err, "Cant create consumer group")
	}
	defer cg.Close()

	err = cg.Consume(ctx, []string{topic}, newCommitConsumerGroup(offsets, topic))
	if err != nil {
		return errors.Wrap(err, "Cant consume")
	}

	return nil
}

func newCommitConsumerGroup(
	offsets []models.Offset,
	topic string,
) *commitConsumerGroup {
	return &commitConsumerGroup{
		offsets: offsets,
		topic:   topic,
	}
}

type commitConsumerGroup struct {
	offsets []models.Offset
	topic   string
}

func (c *commitConsumerGroup) Setup(session sarama.ConsumerGroupSession) error {
	for _, o := range c.offsets {
		session.MarkOffset(c.topic, o.Partition, o.Offset, "")
		session.ResetOffset(c.topic, o.Partition, o.Offset, "")
	}

	return nil
}

func (c *commitConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *commitConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return nil
}
