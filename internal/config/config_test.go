package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_FromFile(t *testing.T) {
	conf, err := FromFile("./testdata/spinix.yml")
	assert.Nil(t, err)
	assert.NotNil(t, conf)

	// cluster
	assert.Equal(t, "127.0.0.1", conf.Cluster.GRPCAddr)
	assert.Equal(t, 1234, conf.Cluster.GRPCPort)
}
