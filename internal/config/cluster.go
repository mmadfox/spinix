package config

import (
	"net"
	"strconv"

	"github.com/mmadfox/spinix/internal/cluster"
)

type clusterConf struct {
	cluster.Options `yaml:",inline"`
}

func (c *Config) ClusterAddr() string {
	return net.JoinHostPort(c.Cluster.BindAddr, strconv.Itoa(c.Cluster.BindPort))
}
