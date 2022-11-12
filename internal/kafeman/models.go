package kafeman

type Topic struct {
	Name       string
	Partitions int
	Replicas   int
}

func NewGroup() Group {
	return Group{
		Members: make([]Member, 0),
		Offsets: make(map[string][]Offset),
	}
}

type Group struct {
	GroupID string `json:"group_id"`
	State   string `json:"state"`
	/// TODO:
	Offsets map[string][]Offset `json:"offsets"`
	// Protocol     string
	// ProtocolType string
	Members []Member `json:"members"`
}

type Member struct {
	Host        string       `json:"host"`
	ID          string       `json:"id"`
	Assignments []Assignment `json:"assignments"`
}

type Assignment struct {
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
}

type Offset struct {
	Partition      int32 `json:"partition"`
	Offset         int64 `json:"offset"`
	HightWatermark int64 `json:"hight_watermark"`
	Lag            int64 `json:"lag"`
}
