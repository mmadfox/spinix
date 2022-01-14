package cluster

type Options struct {
	GRPCAddr string `yaml:"grpc_addr"`
	GRPCPort int    `yaml:"grpc_port"`
}
