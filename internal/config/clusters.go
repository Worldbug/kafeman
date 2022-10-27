package config

type Clusters []Cluster

type Cluster struct {
	Name      string   `yaml:"name"`
	Brokers   []string `yaml:"brokers"`
	ProtoType string   `yaml:"proto_type"`
}
