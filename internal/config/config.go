package config

import (
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

func init() {
	Config = LoadConfig()
}

var Config *Configuration

const (
	defaultConfigDir  = `.config/kafeman`
	defaultConfigName = "config.yaml"
)

type Configuration struct {
	CurrentCluster string   `mapstructure:"current_cluster"`
	Clusters       Clusters `mapstructure:"clusters"`
	Topics         []Topic  `mapstructure:"topics"`
	Quiet          bool     `mapstructure:"-"`
	FailTolerance  bool     `mapstructure:"-"`
}

func (c *Configuration) GetTopicByName(name string) (Topic, bool) {
	for _, topic := range c.Topics {
		if topic.Name == name {
			return topic, true
		}
	}

	return Topic{}, false
}

func (c *Configuration) GetCurrentCluster() Cluster {
	for _, cluster := range c.Clusters {
		if cluster.Name == c.CurrentCluster {
			return cluster
		}
	}

	return Cluster{}
}

func (c *Configuration) SetCurrentCluster(name string) {
	viper.Set("current_cluster", name)
	c.CurrentCluster = name
}

func LoadConfig() *Configuration {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(getDefaultConfigPath())
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	config := &Configuration{}

	err = viper.Unmarshal(config)
	if err != nil {
		panic(err)
	}

	return config
}

func ExportConfig(path string) error {
	return SaveConfig()
}

func SaveConfig() error {
	return viper.WriteConfig()
}

func getDefaultConfigPath() string {
	home, err := homedir.Dir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(home, defaultConfigDir, defaultConfigName)
}

// TODO: FIXME:
// func GenerateConfig() *Configuration {
// 	return &Configuration{
// 		CurrentCluster: "prod",
// 		Clusters: Clusters{
// 			Cluster{
// 				Name: "prod",
// 				Brokers: []string{
// 					"broker_1:9092",
// 					"broker_2:9092",
// 					"broker_3:9092",
// 				},
// 			},
// 		},
// 		Topics: map[string]Topic{
// 			"service_topic": {
// 				ProtoType: "service_event",
// 				ProtoPaths: []string{
// 					"./service_protos/",
// 					"./additional_protos/",
// 				},
// 			},
// 		},
// 	}
// }
