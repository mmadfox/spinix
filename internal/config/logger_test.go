package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_NewLogger(t *testing.T) {
	conf, err := FromFile("./testdata/logger.yml")
	assert.Nil(t, err)
	assert.NotNil(t, conf)
	logger, err := conf.BuildLogger()
	assert.Nil(t, err)
	assert.NotNil(t, logger)
}
