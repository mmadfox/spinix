package config

import (
	"io/ioutil"

	"github.com/mmadfox/spinix/internal/cluster"

	"gopkg.in/yaml.v3"
)

type Config struct {
	GRPC    grpc        `yaml:"grpc"`
	Logger  logger      `yaml:"logger"`
	Cluster clusterConf `yaml:"cluster"`
}

func newConfig() *Config {
	return &Config{}
}

func (c *Config) prepare() {
	c.Cluster.GRPCServerAddr = c.GRPC.ServerAddr
	c.Cluster.GRPCServerPort = c.GRPC.ServerPort
}

func (c *Config) ClusterOptions() *cluster.Options {
	return &c.Cluster.Options
}

func (c *Config) sanitize() {

}

func (c *Config) validate() error {
	return nil
}

func FromBytes(data []byte) (*Config, error) {
	conf := newConfig()
	if err := yaml.Unmarshal(data, conf); err != nil {
		return nil, err
	}
	conf.sanitize()
	conf.prepare()
	if err := conf.validate(); err != nil {
		return nil, err
	}
	return conf, nil
}

func FromFile(filename string) (*Config, error) {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return FromBytes(raw)
}
