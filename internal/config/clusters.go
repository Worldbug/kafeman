package config

type Clusters []Cluster

type Cluster struct {
	Name              string   `mapstructure:"name"`
	Brokers           []string `mapstructure:"brokers"`
	Version           string   `mapstructure:"version"`
	SASL              *SASL    `mapstructure:"SASL"`
	TLS               *TLS     `mapstructure:"TLS"`
	SecurityProtocol  string   `mapstructure:"security-protocol"`
	SchemaRegistryURL string   `mapstructure:"schema-registry-url"`
}

type SASL struct {
	Mechanism    string `mapstructure:"mechanism"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	ClientID     string `mapstructure:"clientID"`
	ClientSecret string `mapstructure:"clientSecret"`
	TokenURL     string `mapstructure:"tokenURL"`
	Token        string `mapstructure:"token"`
}

type TLS struct {
	Cafile        string
	Clientfile    string
	Clientkeyfile string
	Insecure      bool
}
