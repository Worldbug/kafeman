package config

import (
	"os"

	"gopkg.in/yaml.v2"
)

const (
	configLocation = "~/.protokaf/config.yml"
)

type Config struct {
	CurrentCluster string   `yaml:"current_cluster"`
	Clusters       Clusters `yaml:"clusters"`
}

func LoadConfig(configPath string) (Config, error) {
	cfg := Config{}

	path := valueOrDefault(configPath, configLocation)
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
