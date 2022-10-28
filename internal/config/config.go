package config

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"gopkg.in/yaml.v2"
)

type Config struct {
	CurrentCluster string   `yaml:"current_cluster"`
	Clusters       Clusters `yaml:"clusters"`
	Protobuf       Protobuf `yaml:"protobuf"`
}

func (c *Config) GetCurrentCluster() Cluster {
	for _, cluster := range c.Clusters {
		if cluster.Name == c.CurrentCluster {
			return cluster
		}
	}

	return Cluster{}
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

	return filepath.Join(home, ".protokaf", "config.yml")
}
