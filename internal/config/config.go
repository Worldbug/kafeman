package config

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v2"
)

const (
	defaultConfigDir  = `/.config/kafeman`
	defaultConfigName = "config.yml"
)

type Config struct {
	CurrentCluster string           `yaml:"current_cluster"`
	Clusters       Clusters         `yaml:"clusters"`
	Topics         map[string]Topic `yaml:"topics"`
}

func (c *Config) GetCurrentCluster() Cluster {
	for _, cluster := range c.Clusters {
		if cluster.Name == c.CurrentCluster {
			return cluster
		}
	}

	return Cluster{}
}

func (c *Config) SetCurrentCluster(name string) {
	c.CurrentCluster = name
}

func GenerateConfig() Config {
	return Config{
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

func LoadConfig(configPath string) (Config, error) {
	cfg := Config{}

	path := valueOrDefault(configPath, getDefaultConfigPath())
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return cfg, err
	}

	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&cfg)

	return cfg, err
}

func ExportConfig(path string) error {
	c := GenerateConfig()
	return SaveConfig(c, path)
}

func SaveConfig(config Config, path string) error {
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

func valueOrDefault(val, def string) string {
	if val != "" {
		return val
	}

	return def
}

func getDefaultConfigPath() string {
	home, err := homedir.Dir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(home, defaultConfigDir, defaultConfigName)
}
