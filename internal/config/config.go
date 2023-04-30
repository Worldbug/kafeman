package config

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/worldbug/kafeman/internal/utils"
	"gopkg.in/yaml.v3"
)

var ConfigPath = getDefaultConfigPath()
var Config = readConfiguration()

type Configuration struct {
	CurrentCluster string    `yaml:"current_cluster"`
	Clusters       []Cluster `yaml:"clusters"`
	Topics         []Topic   `yaml:"topics"`
	Quiet          bool      `yaml:"-"`
	FailTolerance  bool      `yaml:"-"`
}

func SetCurrentCluster(name string) bool {
	return Config.SetCurrentCluster(name)
}

func (c *Configuration) SetCurrentCluster(name string) bool {
	_, ok := c.GetClusterByName(name)
	if !ok {
		return false
	}

	c.CurrentCluster = name
	return true
}

func GetCurrentCluster() Cluster {
	return Config.GetCurrentCluster()
}

func (c *Configuration) GetCurrentCluster() Cluster {
	cluster, ok := c.GetClusterByName(Config.CurrentCluster)
	if !ok {
		return Cluster{}
	}

	return cluster
}

func GetClusterByName(name string) (Cluster, bool) {
	return Config.GetClusterByName(name)
}

func (c *Configuration) GetClusterByName(name string) (Cluster, bool) {
	cluster, ok := utils.SliceToMap(Config.Clusters, Cluster.GetName)[name]
	return cluster, ok
}

func SetCluster(cluster Cluster) {
	Config.SetCluster(cluster)
}

func (c *Configuration) SetCluster(cluster Cluster) {
	clusters := utils.SliceToMap(Config.Clusters, Cluster.GetName)
	clusters[cluster.GetName()] = cluster
	Config.Clusters = utils.MapToSlice(clusters)
}

func GetTopicByName(name string) (Topic, bool) {
	return Config.GetTopicByName(name)
}

func (c *Configuration) GetTopicByName(name string) (Topic, bool) {
	topic, ok := utils.SliceToMap(Config.Topics, Topic.GetName)[name]
	return topic, ok
}

func SetTopic(topic Topic) {
	Config.SetTopic(topic)
}

func (c *Configuration) SetTopic(topic Topic) {
	topics := utils.SliceToMap(Config.Topics, Topic.GetName)
	topics[topic.GetName()] = topic
	Config.Topics = utils.MapToSlice(topics)
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

func ReadConfiguration() {
	Config = readConfiguration()
}

func readConfiguration() *Configuration {
	config := &Configuration{}
	file, err := os.ReadFile(ConfigPath)
	if err != nil {
		// TODO: error handling
		return config
	}

	err = yaml.Unmarshal(file, config)
	if err != nil {
		panic(err)
	}

	return config
}

func WriteConfiguration() error {
	file, err := yaml.Marshal(Config)
	if err != nil {
		return err
	}

	err = os.WriteFile(ConfigPath, file, 0644)
	if err != nil {
		return err
	}

	return nil
}

func getDefaultConfigPath() string {
	home, err := homedir.Dir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(home, ".config", "kafeman", "config.yaml")
}
