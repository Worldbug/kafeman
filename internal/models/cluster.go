package models

func NewClusterInfo() ClusterInfo {
	return ClusterInfo{
		Brokers: make([]BrokerInfo, 0),
	}
}

type ClusterInfo struct {
	Brokers []BrokerInfo `json:"brokers"`
}

func NewBrokerInfo(
	id int32,
	addr string,
	isController bool,
) BrokerInfo {
	return BrokerInfo{
		ID:           id,
		Addr:         addr,
		IsController: isController,
	}
}

type BrokerInfo struct {
	ID           int32  `json:"id"`
	Addr         string `json:"addr"`
	IsController bool   `json:"is_controller"`
}
