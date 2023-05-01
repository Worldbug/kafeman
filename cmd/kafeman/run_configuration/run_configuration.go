package run_configuration

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/worldbug/kafeman/internal/config"
	"gopkg.in/yaml.v2"
)

var ConfigPath = getDefaultConfigPath()
var Config = readConfiguration()

func SetCurrentCluster(name string) bool {
	return Config.SetCurrentCluster(name)
}

func GetCurrentCluster() config.Cluster {
	return Config.GetCurrentCluster()
}

func GetClusterByName(name string) (config.Cluster, bool) {
	return Config.GetClusterByName(name)
}

func SetCluster(cluster config.Cluster) {
	Config.SetCluster(cluster)
}

func GetTopicByName(name string) (config.Topic, bool) {
	return Config.GetTopicByName(name)
}

func SetTopic(topic config.Topic) {
	Config.SetTopic(topic)
}

func ReadConfiguration() {
	Config = readConfiguration()
}

func readConfiguration() *config.Configuration {
	config := &config.Configuration{}
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
