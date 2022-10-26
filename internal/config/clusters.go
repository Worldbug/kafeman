package config

type Clusters []Cluster

type Cluster struct {
	Name       string   `yaml:"name"`
	Brokers    []string `yaml:"brokers"`
	ProtoModel string   `yaml:"proto_model"`
}
