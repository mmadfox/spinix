package config

import (
	"net"
	"strconv"
)

type grpc struct {
	ServerAddr string `yaml:"server_addr"`
	ServerPort int    `yaml:"server_port"`
}

func (c *Config) GRPCAddr() string {
	return net.JoinHostPort(c.GRPC.ServerAddr, strconv.Itoa(c.GRPC.ServerPort))
}
