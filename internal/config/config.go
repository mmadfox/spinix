package config

import (
	"io/ioutil"

	"github.com/mmadfox/spinix/internal/cluster"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Cluster cluster.Options `yaml:"cluster"`
}

func FromBytes(data []byte) (*Config, error) {
	var conf Config
	if err := yaml.Unmarshal(data, &conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

func FromFile(filename string) (*Config, error) {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return FromBytes(raw)
}
