package cluster

import (
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"

	"github.com/hashicorp/memberlist"
)

type Options struct {
	GRPCServerAddr string `yaml:"grpc_server_addr"`
	GRPCServerPort int    `yaml:"grpc_server_port"`

	GRPCClientIdleTimeout     time.Duration `yaml:"grpc_client_idle_timeout"`
	GRPCClientMaxLifeDuration time.Duration `yaml:"grpc_client_max_life_duration"`
	GRPCClientInitPoolCount   int           `yaml:"grpc_client_pool_init_count"`
	GRPCClientPoolCapacity    int           `yaml:"grpc_client_pool_capacity"`

	H3DistLevel    int    `yaml:"h3dist_level"`
	H3DistVNodes   uint64 `yaml:"h3dist_vnodes"`
	H3DistReplicas int    `yaml:"h3dist_replicas"`

	JoinRetryInterval time.Duration `yaml:"join_retry_interval"`
	MaxJoinAttempts   int           `yaml:"max_join_attempts"`
	Peers             []string      `yaml:"peers"`

	MemberlistDefaultConf   string         `yaml:"memberlist_default_conf"`
	BindAddr                string         `yaml:"memberlist_bind_addr"`
	BindPort                int            `yaml:"memberlist_bind_port"`
	AdvertiseAddr           *string        `yaml:"memberlist_advertise_addr"`
	AdvertisePort           *int           `yaml:"memberlist_advertise_port"`
	EnableCompression       *bool          `yaml:"memberlist_enable_compression"`
	IndirectChecks          *int           `yaml:"memberlist_indirect_checks"`
	RetransmitMult          *int           `yaml:"memberlist_retransmit_mult"`
	SuspicionMult           *int           `yaml:"memberlist_suspicion_mult"`
	TCPTimeout              *time.Duration `yaml:"memberlist_tcp_timeout"`
	PushPullInterval        *time.Duration `yaml:"memberlist_push_pull_interval"`
	ProbeTimeout            *time.Duration `yaml:"memberlist_probe_timeout"`
	ProbeInterval           *time.Duration `yaml:"memberlist_probe_interval"`
	GossipInterval          *time.Duration `yaml:"memberlist_gossip_interval"`
	GossipToTheDeadTime     *time.Duration `yaml:"memberlist_gossip_dead_time"`
	SuspicionMaxTimeoutMult *int           `yaml:"memberlist_suspicion_max_timeout_mult"`
	AwarenessMaxMultiplier  *int           `yaml:"memberlist_awareness_max_multiplier"`
	GossipNodes             *int           `yaml:"memberlist_gossip_nodes"`
	GossipVerifyIncoming    *bool          `yaml:"memberlist_gossip_verify_incoming"`
	GossipVerifyOutgoing    *bool          `yaml:"memberlist_gossip_verify_outgoing"`
	DNSConfigPath           *string        `yaml:"memberlist_dns_config_path"`
	HandoffQueueDepth       *int           `yaml:"memberlist_handoff_queue_depth"`
	UDPBufferSize           *int           `yaml:"memberlist_udp_buffer_size"`

	GRPCClientDialOpts []grpc.DialOption `yaml:"-"`
}

func toMemberlistConf(o *Options) *memberlist.Config {
	var conf *memberlist.Config
	switch o.MemberlistDefaultConf {
	case "local":
		conf = memberlist.DefaultLocalConfig()
	case "wan":
		conf = memberlist.DefaultWANConfig()
	case "lan":
		conf = memberlist.DefaultLANConfig()
	default:
		conf = memberlist.DefaultLocalConfig()
	}
	if len(o.BindAddr) > 0 {
		conf.BindAddr = o.BindAddr
	}
	if o.BindPort > 0 {
		conf.BindPort = o.BindPort
	}
	if o.EnableCompression != nil {
		conf.EnableCompression = *o.EnableCompression
	}
	if o.IndirectChecks != nil {
		conf.IndirectChecks = *o.IndirectChecks
	}
	if o.RetransmitMult != nil {
		conf.RetransmitMult = *o.RetransmitMult
	}
	if o.SuspicionMult != nil {
		conf.SuspicionMult = *o.SuspicionMult
	}
	if o.TCPTimeout != nil {
		conf.TCPTimeout = *o.TCPTimeout
	}
	if o.PushPullInterval != nil {
		conf.PushPullInterval = *o.PushPullInterval
	}
	if o.ProbeTimeout != nil {
		conf.ProbeTimeout = *o.ProbeTimeout
	}
	if o.ProbeInterval != nil {
		conf.ProbeInterval = *o.ProbeInterval
	}
	if o.GossipInterval != nil {
		conf.GossipInterval = *o.GossipInterval
	}
	if o.GossipToTheDeadTime != nil {
		conf.GossipToTheDeadTime = *o.GossipToTheDeadTime
	}
	if o.AdvertiseAddr != nil {
		conf.AdvertiseAddr = *o.AdvertiseAddr
	}
	if o.AdvertisePort != nil {
		conf.AdvertisePort = *o.AdvertisePort
	}
	if o.SuspicionMaxTimeoutMult != nil {
		conf.SuspicionMaxTimeoutMult = *o.SuspicionMaxTimeoutMult
	}
	if o.AwarenessMaxMultiplier != nil {
		conf.AwarenessMaxMultiplier = *o.AwarenessMaxMultiplier
	}
	if o.GossipNodes != nil {
		conf.GossipNodes = *o.GossipNodes
	}
	if o.GossipVerifyIncoming != nil {
		conf.GossipVerifyIncoming = *o.GossipVerifyIncoming
	}
	if o.GossipVerifyOutgoing != nil {
		conf.GossipVerifyOutgoing = *o.GossipVerifyOutgoing
	}
	if o.DNSConfigPath != nil {
		conf.DNSConfigPath = *o.DNSConfigPath
	}
	if o.HandoffQueueDepth != nil {
		conf.HandoffQueueDepth = *o.HandoffQueueDepth
	}
	if o.UDPBufferSize != nil {
		conf.UDPBufferSize = *o.UDPBufferSize
	}
	return conf
}

func joinAddrPort(addr string, port int) string {
	return net.JoinHostPort(addr, strconv.Itoa(port))
}
