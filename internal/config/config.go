package config

type SASL struct {
	Mechanism    string `yaml:"mechanism"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	ClientID     string `yaml:"clientID"`
	ClientSecret string `yaml:"clientSecret"`
	TokenURL     string `yaml:"tokenURL"`
	Token        string `yaml:"token"`
}

type TLS struct {
	Cafile        string
	Clientfile    string
	Clientkeyfile string
	Insecure      bool
}

type Cluster struct {
	Name              string
	Version           string   `yaml:"version"`
	Brokers           []string `yaml:"brokers"`
	SASL              *SASL    `yaml:"SASL"`
	TLS               *TLS     `yaml:"TLS"`
	SecurityProtocol  string   `yaml:"security-protocol"`
	SchemaRegistryURL string   `yaml:"schema-registry-url"`
}

// TODO: include
type Protobuf struct {
	VendoredProtos []string `yaml:"vendored_protos"`
}

type Config struct {
	CurrentCluster  string `yaml:"current-cluster"`
	ClusterOverride string
	Clusters        []*Cluster `yaml:"clusters"`
}
