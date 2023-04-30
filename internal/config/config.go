package config

import (
	"github.com/worldbug/kafeman/internal/utils"
)

type Configuration struct {
	CurrentCluster string    `yaml:"current_cluster"`
	Clusters       []Cluster `yaml:"clusters"`
	Topics         []Topic   `yaml:"topics"`
	Quiet          bool      `yaml:"-"`
	FailTolerance  bool      `yaml:"-"`
}

func (c *Configuration) SetCurrentCluster(name string) bool {
	_, ok := c.GetClusterByName(name)
	if !ok {
		return false
	}

	c.CurrentCluster = name
	return true
}

func (c *Configuration) GetCurrentCluster() Cluster {
	cluster, ok := c.GetClusterByName(c.CurrentCluster)
	if !ok {
		return Cluster{}
	}

	return cluster
}

func (c *Configuration) GetClusterByName(name string) (Cluster, bool) {
	cluster, ok := utils.SliceToMap(c.Clusters, Cluster.GetName)[name]
	return cluster, ok
}

func (c *Configuration) SetCluster(cluster Cluster) {
	clusters := utils.SliceToMap(c.Clusters, Cluster.GetName)
	clusters[cluster.GetName()] = cluster
	c.Clusters = utils.MapToSlice(clusters)
}

func (c *Configuration) GetTopicByName(name string) (Topic, bool) {
	topic, ok := utils.SliceToMap(c.Topics, Topic.GetName)[name]
	return topic, ok
}

func (c *Configuration) SetTopic(topic Topic) {
	topics := utils.SliceToMap(c.Topics, Topic.GetName)
	topics[topic.GetName()] = topic
	c.Topics = utils.MapToSlice(topics)
}

type Cluster struct {
	Name              string   `yaml:"name,omitempty"`
	Brokers           []string `yaml:"brokers,omitempty"`
	Version           string   `yaml:"version,omitempty"`
	SASL              *SASL    `yaml:"sasl,omitempty"`
	TLS               *TLS     `yaml:"tls,omitempty"`
	SecurityProtocol  string   `yaml:"security_protocol,omitempty"`
	SchemaRegistryURL string   `yaml:"schema_registry_url,omitempty"`
}

func (c Cluster) GetName() string {
	return c.Name
}

type SASL struct {
	Mechanism    string `yaml:"mechanism,omitempty"`
	Username     string `yaml:"username,omitempty"`
	Password     string `yaml:"password,omitempty"`
	ClientID     string `yaml:"client_id,omitempty"`
	ClientSecret string `yaml:"client_secret,omitempty"`
	TokenURL     string `yaml:"token_url,omitempty"`
	Token        string `yaml:"token,omitempty"`
}

type TLS struct {
	Cafile        string `yaml:"cafile,omitempty"`
	Clientfile    string `yaml:"clientfile,omitempty"`
	Clientkeyfile string `yaml:"clientkeyfile,omitempty"`
	Insecure      bool   `yaml:"insecure,omitempty"`
}

type Encoding string

const (
	MSGPack  Encoding = "msgpack"
	Protobuf Encoding = "protobuf"
	Avro     Encoding = "avro"
	RAW      Encoding = "raw"
	Base64   Encoding = "base64"
)

type Topic struct {
	Name          string   `yaml:"name"`
	Encoding      Encoding `yaml:"encoding,omitempty"`
	ProtoType     string   `yaml:"proto_type,omitempty"`
	ProtoPaths    []string `yaml:"proto_paths,omitempty"`
	AvroSchemaURL string   `yaml:"avro_schema_url,omitempty"`
	AvroSchemaID  int      `yaml:"avro_schema_id,omitempty"`
}

func (t Topic) GetName() string {
	return t.Name
}
