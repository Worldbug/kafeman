package admin

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/worldbug/kafeman/cmd/kafeman/run_configuration"
	"github.com/worldbug/kafeman/internal/models"
	"github.com/worldbug/kafeman/internal/sarama_config"
)

func (a *Admin) GetClusterInfo(ctx context.Context) (models.ClusterInfo, error) {
	info := models.NewClusterInfo()
	config, err := sarama_config.GetSaramaFromConfig(a.config)
	if err != nil {
		return info, errors.Wrap(err, "Cant create sarama config")
	}

	adm, _ := sarama.NewClusterAdmin(run_configuration.GetCurrentCluster().Brokers, config)
	br, cid, _ := adm.DescribeCluster()
	for _, b := range br {
		info.Brokers = append(info.Brokers, models.NewBrokerInfo(
			b.ID(), b.Addr(), b.ID() == cid,
		))
	}

	return info, nil
}
