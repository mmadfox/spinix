package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_FromFile(t *testing.T) {
	conf, err := FromFile("./testdata/spinix.yml")
	assert.Nil(t, err)
	assert.NotNil(t, conf)

	// cluster
	assert.Equal(t, "127.0.0.1", conf.Cluster.GRPCAddr)
	assert.Equal(t, 9000, conf.Cluster.GRPCPort)
	assert.Equal(t, 50*time.Millisecond, conf.Cluster.JoinRetryInterval)
	assert.Equal(t, 3, conf.Cluster.MaxJoinAttempts)
	assert.Len(t, conf.Cluster.Peers, 3)
	assert.Equal(t, "127.0.0.1", conf.Cluster.BindAddr)
	assert.Equal(t, 11000, conf.Cluster.BindPort)
	assert.Equal(t, "127.0.0.1", *conf.Cluster.AdvertiseAddr)
	assert.Equal(t, 12000, *conf.Cluster.AdvertisePort)
	assert.True(t, *conf.Cluster.EnableCompression)
	assert.Equal(t, 3, *conf.Cluster.IndirectChecks)
	assert.Equal(t, 4, *conf.Cluster.RetransmitMult)
	assert.Equal(t, 4, *conf.Cluster.SuspicionMult)
	assert.Equal(t, 10*time.Second, *conf.Cluster.TCPTimeout)
	assert.Equal(t, 30*time.Second, *conf.Cluster.PushPullInterval)
	assert.Equal(t, 500*time.Millisecond, *conf.Cluster.ProbeTimeout)
	assert.Equal(t, 1*time.Second, *conf.Cluster.ProbeInterval)
	assert.Equal(t, 200*time.Millisecond, *conf.Cluster.GossipInterval)
	assert.Equal(t, 30*time.Second, *conf.Cluster.GossipToTheDeadTime)
	assert.Equal(t, 6, *conf.Cluster.SuspicionMaxTimeoutMult)
	assert.Equal(t, 8, *conf.Cluster.AwarenessMaxMultiplier)
	assert.Equal(t, 3, *conf.Cluster.GossipNodes)
	assert.True(t, *conf.Cluster.GossipVerifyIncoming)
	assert.True(t, *conf.Cluster.GossipVerifyOutgoing)
	assert.Equal(t, "/etc/resolv.conf", *conf.Cluster.DNSConfigPath)
	assert.Equal(t, 1024, *conf.Cluster.HandoffQueueDepth)
	assert.Equal(t, 1400, *conf.Cluster.UDPBufferSize)
}
