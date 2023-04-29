package config

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

func init() {
	Config = LoadConfig()
}

var Config *Configuration

const (
	defaultConfigDir  = `.config/kafeman`
	defaultConfigName = "config.yml"
)

type Configuration struct {
	CurrentCluster string           `mapstructure:"current_cluster"`
	Clusters       Clusters         `mapstructure:"clusters"`
	Topics         map[string]Topic `mapstructure:"topics"`
	Quiet          bool
	FailTolerance  bool
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
	c.CurrentCluster = name
}

func GenerateConfig() *Configuration {
	return &Configuration{
		CurrentCluster: "prod",
		Clusters: Clusters{
			Cluster{
				Name: "prod",
				Brokers: []string{
					"broker_1:9092",
					"broker_2:9092",
					"broker_3:9092",
				},
			},
		},
		Topics: map[string]Topic{
			"service_topic": {
				ProtoType: "service_event",
				ProtoPaths: []string{
					"./service_protos/",
					"./additional_protos/",
				},
			},
		},
	}
}

func LoadConfig() *Configuration {
	viper.SetConfigFile(getDefaultConfigPath())
	err := viper.ReadInConfig()
	if err != nil {
		return &Configuration{}
	}

	config := &Configuration{}

	err = viper.Unmarshal(config)
	if err != nil {
		panic(err)
	}

	return config
}

func ExportConfig(path string) error {
	c := GenerateConfig()
	return SaveConfig(c, path)
}

func SaveConfig(config *Configuration, path string) error {
	if path == "" {
		home, err := homedir.Dir()
		if err != nil {
			return err
		}

		configDir := filepath.Join(home, defaultConfigDir)
		err = os.MkdirAll(configDir, 0755)
		if err != nil {
			return err
		}

		path = filepath.Join(configDir, defaultConfigName)

	}

	file, err := os.OpenFile(path, os.O_TRUNC|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	encoder := yaml.NewEncoder(file)
	return encoder.Encode(&config)
}

func getDefaultConfigPath() string {
	home, err := homedir.Dir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(home, defaultConfigDir, defaultConfigName)
}
