package models

func NewTopicInfo() TopicInfo {
	return TopicInfo{
		Consumers: []TopicConsumerInfo{},
	}
}

type TopicInfo struct {
	TopicName string              `json:"topic_name,omitempty"`
	Consumers []TopicConsumerInfo `json:"consumers,omitempty"`
	// TODO: add more info
	// Partitions
}

type TopicConsumerInfo struct {
	Name         string `json:"name,omitempty"`
	MembersCount int    `json:"members_count,omitempty"`
	// State ?
}
