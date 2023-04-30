package config

type Cluster struct {
	Name              string   `mapstructure:"name,omitempty"`
	Brokers           []string `mapstructure:"brokers,omitempty"`
	Version           string   `mapstructure:"version,omitempty"`
	SASL              *SASL    `mapstructure:"sasl,omitempty"`
	TLS               *TLS     `mapstructure:"tls,omitempty"`
	SecurityProtocol  string   `mapstructure:"security_protocol,omitempty"`
	SchemaRegistryURL string   `mapstructure:"schema_registry_url,omitempty"`
}

type SASL struct {
	Mechanism    string `mapstructure:"mechanism,omitempty"`
	Username     string `mapstructure:"username,omitempty"`
	Password     string `mapstructure:"password,omitempty"`
	ClientID     string `mapstructure:"client_id,omitempty"`
	ClientSecret string `mapstructure:"client_secret,omitempty"`
	TokenURL     string `mapstructure:"token_url,omitempty"`
	Token        string `mapstructure:"token,omitempty"`
}

type TLS struct {
	Cafile        string `mapstructure:"cafile,omitempty"`
	Clientfile    string `mapstructure:"clientfile,omitempty"`
	Clientkeyfile string `mapstructure:"clientkeyfile,omitempty"`
	Insecure      bool   `mapstructure:"insecure,omitempty"`
}
