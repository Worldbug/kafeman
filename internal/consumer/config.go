package consumer

import (
	"github.com/Shopify/sarama"
)

func getConfig() (saramaConfig *sarama.Config) {
	saramaConfig = sarama.NewConfig()
	saramaConfig.Version = sarama.V1_1_0_0
	saramaConfig.Producer.Return.Successes = true

	// cluster := currentCluster
	// if cluster.Version != "" {
	// 	parsedVersion, err := sarama.ParseKafkaVersion(cluster.Version)
	// 	if err != nil {
	// 		errorExit("Unable to parse Kafka version: %v\n", err)
	// 	}
	// 	saramaConfig.Version = parsedVersion
	// }
	// if cluster.SASL != nil {
	// 	saramaConfig.Net.SASL.Enable = true
	// 	if cluster.SASL.Mechanism != "OAUTHBEARER" {
	// 		saramaConfig.Net.SASL.User = cluster.SASL.Username
	// 		saramaConfig.Net.SASL.Password = cluster.SASL.Password
	// 	}
	// }
	// if cluster.TLS != nil && cluster.SecurityProtocol != "SASL_SSL" {
	// 	saramaConfig.Net.TLS.Enable = true
	// 	tlsConfig := &tls.Config{
	// 		InsecureSkipVerify: cluster.TLS.Insecure,
	// 	}

	// 	if cluster.TLS.Cafile != "" {
	// 		caCert, err := ioutil.ReadFile(cluster.TLS.Cafile)
	// 		if err != nil {
	// 			errorExit("Unable to read Cafile :%v\n", err)
	// 		}
	// 		caCertPool := x509.NewCertPool()
	// 		caCertPool.AppendCertsFromPEM(caCert)
	// 		tlsConfig.RootCAs = caCertPool
	// 	}

	// 	if cluster.TLS.Clientfile != "" && cluster.TLS.Clientkeyfile != "" {
	// 		clientCert, err := ioutil.ReadFile(cluster.TLS.Clientfile)
	// 		if err != nil {
	// 			errorExit("Unable to read Clientfile :%v\n", err)
	// 		}
	// 		clientKey, err := ioutil.ReadFile(cluster.TLS.Clientkeyfile)
	// 		if err != nil {
	// 			errorExit("Unable to read Clientkeyfile :%v\n", err)
	// 		}

	// 		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
	// 		if err != nil {
	// 			errorExit("Unable to create KeyPair: %v\n", err)
	// 		}
	// 		tlsConfig.Certificates = []tls.Certificate{cert}

	// 		// nolint
	// 		tlsConfig.BuildNameToCertificate()
	// 	}
	// 	saramaConfig.Net.TLS.Config = tlsConfig
	// }
	// if cluster.SecurityProtocol == "SASL_SSL" {
	// 	saramaConfig.Net.TLS.Enable = true
	// 	if cluster.TLS != nil {
	// 		tlsConfig := &tls.Config{
	// 			InsecureSkipVerify: cluster.TLS.Insecure,
	// 		}
	// 		if cluster.TLS.Cafile != "" {
	// 			caCert, err := ioutil.ReadFile(cluster.TLS.Cafile)
	// 			if err != nil {
	// 				fmt.Println(err)
	// 				os.Exit(1)
	// 			}
	// 			caCertPool := x509.NewCertPool()
	// 			caCertPool.AppendCertsFromPEM(caCert)
	// 			tlsConfig.RootCAs = caCertPool
	// 		}
	// 		saramaConfig.Net.TLS.Config = tlsConfig

	// 	} else {
	// 		saramaConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: false}
	// 	}
	// }
	// if cluster.SecurityProtocol == "SASL_SSL" || cluster.SecurityProtocol == "SASL_PLAINTEXT" {
	// 	if cluster.SASL.Mechanism == "SCRAM-SHA-512" {
	// 		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
	// 		saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	// 	} else if cluster.SASL.Mechanism == "SCRAM-SHA-256" {
	// 		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	// 		saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
	// 	} else if cluster.SASL.Mechanism == "OAUTHBEARER" {
	// 		//Here setup get token function
	// 		saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeOAuth)
	// 		saramaConfig.Net.SASL.TokenProvider = newTokenProvider()

	// 	}
	// }
	return saramaConfig
}
