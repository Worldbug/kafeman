package config

type Clusters []Cluster

type Cluster struct {
	Name              string   `yaml:"name"`
	Brokers           []string `yaml:"brokers"`
	Version           string   `yaml:"version"`
	SASL              *SASL    `yaml:"SASL"`
	TLS               *TLS     `yaml:"TLS"`
	SecurityProtocol  string   `yaml:"security-protocol"`
	SchemaRegistryURL string   `yaml:"schema-registry-url"`
}

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
