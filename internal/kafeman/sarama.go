package kafeman

import (
	"github.com/Shopify/sarama"
)

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
	// saramaConfig.Consumer.Offsets.
	return saramaConfig
}

// func (k *kafeman) getConsumerGroup(group string) {
// 	conf := k.getSaramaConfig()
//
// 	cg, err := sarama.NewConsumerGroup(k.config.GetCurrentCluster().Brokers, group, conf)
// 	if err != nil {
// 		return
// 	}
//
// 	cg.Consume(context.Background(), []string{"test"}, k)
// }

//
// func (k *kafeman) Setup(_ sarama.ConsumerGroupSession) error {
// 	return nil
// }
//
// func (k *kafeman) Cleanup(_ sarama.ConsumerGroupSession) error {
// 	return nil
// }
//
// func (k *kafeman) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
// 	for {
// 		select {
// 		case <-session.Context().Done():
// 			return nil
// 		case msg := <-claim.Messages():
// 			k.messageHandler(messageFromSarama(msg), true)
// 		}
// 	}
// }
//
