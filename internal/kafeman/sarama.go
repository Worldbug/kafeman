package kafeman

import "github.com/Shopify/sarama"

// TODO: on error
func (k *kafeman) getSaramaAdmin() sarama.ClusterAdmin {
	var admin sarama.ClusterAdmin
	clusterAdmin, err := sarama.NewClusterAdmin(k.config.GetCurrentCluster().Brokers, k.getSaramaConfig())
	if err != nil {
		// errorExit("Unable to get cluster admin: %v\n", err)
		return admin
	}

	return clusterAdmin
}

// TODO:
func (k *kafeman) getSaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true
	return saramaConfig
}
