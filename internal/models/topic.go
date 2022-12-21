package models

func NewTopicInfo() TopicInfo {
	return TopicInfo{}
}

type TopicInfo struct {
	TopicName  string              `json:"topic_name,omitempty"`
	Internal   bool                `json:"internal,omitempty"`
	Compacted  bool                `json:"compacted,omitempty"`
	Partitions []PartitionInfo     `json:"partitions,omitempty"`
	Config     []TopicConfigRecord `json:"config,omitempty"`
}

type TopicConsumers struct {
	Consumers []TopicConsumerInfo `json:"consumers,omitempty"`
}

type TopicConsumerInfo struct {
	Name         string `json:"name,omitempty"`
	MembersCount int    `json:"members_count,omitempty"`
	// State ?
}

type PartitionInfo struct {
	Partition      int32   `json:"partition,omitempty"`
	HightWatermark int64   `json:"hight_watermark,omitempty"`
	Leader         int32   `json:"leader,omitempty"`
	Replicas       int     `json:"replicas,omitempty"`
	ISR            []int32 `json:"isr,omitempty"`
}

type TopicConfigRecord struct {
	Name      string `json:"name,omitempty"`
	Value     string `json:"value,omitempty"`
	ReadOnly  bool   `json:"read_only,omitempty"`
	Sensitive bool   `json:"sensitive,omitempty"`
}
