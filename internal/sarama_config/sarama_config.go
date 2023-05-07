package sarama_config

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/worldbug/kafeman/internal/config"
)

func GetSaramaFromConfig(config *config.Configuration) (*sarama.Config, error) {
	cluster := config.GetCurrentCluster()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true

	if cluster.Version != "" {
		err := initVersion(cluster, saramaConfig)
		if err != nil {
			return saramaConfig, err
		}
	}

	if cluster.SASL != nil {
		initSASL(cluster, saramaConfig)
	}

	if cluster.TLS != nil && cluster.SecurityProtocol != "SASL_SSL" {
		err := initTLS(cluster, saramaConfig)
		if err != nil {
			return saramaConfig, err
		}
	}

	if cluster.SecurityProtocol == "cg" {
		err := initCG(cluster, saramaConfig)
		if err != nil {
			return saramaConfig, err
		}
	}

	if cluster.SecurityProtocol == "SASL_SSL" || cluster.SecurityProtocol == "SASL_PLAINTEXT" {
		if cluster.SASL.Mechanism == "SCRAM-SHA-512" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = XDGSCRAMClientSHA512
			saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		} else if cluster.SASL.Mechanism == "SCRAM-SHA-256" {
			saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = XDGSCRAMClientSHA256
			saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		} else if cluster.SASL.Mechanism == "OAUTHBEARER" {
			saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeOAuth)
			saramaConfig.Net.SASL.TokenProvider = newTokenProvider(cluster)

		}
	}

	return saramaConfig, nil
}

func XDGSCRAMClientSHA512() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
func XDGSCRAMClientSHA256() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }

func initVersion(cluster config.Cluster, saramaConfig *sarama.Config) error {
	parsedVersion, err := sarama.ParseKafkaVersion(cluster.Version)
	if err != nil {
		return errors.Wrap(err, "Unable to parse Kafka version")
	}

	saramaConfig.Version = parsedVersion
	return nil
}

func initTLS(cluster config.Cluster, saramaConfig *sarama.Config) error {
	saramaConfig.Net.TLS.Enable = true
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cluster.TLS.Insecure,
	}

	if cluster.TLS.Cafile != "" {
		caCert, err := os.ReadFile(cluster.TLS.Cafile)
		if err != nil {
			return errors.Wrap(err, "Unable to read Cafile")
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	if cluster.TLS.Clientfile != "" && cluster.TLS.Clientkeyfile != "" {
		clientCert, err := os.ReadFile(cluster.TLS.Clientfile)
		if err != nil {
			return errors.Wrap(err, "Unable to read Cafile")
		}

		clientKey, err := os.ReadFile(cluster.TLS.Clientkeyfile)
		if err != nil {
			return errors.Wrap(err, "Unable to read Clientkeyfile")
		}

		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
		if err != nil {
			return errors.Wrap(err, "Unable to create KeyPair")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	saramaConfig.Net.TLS.Config = tlsConfig

	return nil
}

func initSASL(cluster config.Cluster, saramaConfig *sarama.Config) {
	saramaConfig.Net.SASL.Enable = true
	if cluster.SASL.Mechanism != "OAUTHBEARER" {
		saramaConfig.Net.SASL.User = cluster.SASL.Username
		saramaConfig.Net.SASL.Password = cluster.SASL.Password
	}
}

func initCG(cluster config.Cluster, saramaConfig *sarama.Config) error {
	saramaConfig.Net.TLS.Enable = true
	if cluster.TLS != nil {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cluster.TLS.Insecure,
		}
		if cluster.TLS.Cafile != "" {
			caCert, err := os.ReadFile(cluster.TLS.Cafile)
			if err != nil {
				return errors.Wrap(err, "Unable to read Cafile")
			}

			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}
		saramaConfig.Net.TLS.Config = tlsConfig

		return nil
	}

	saramaConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: false}
	return nil
}
