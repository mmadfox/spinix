package config

import (
	"go.uber.org/zap"
)

type logger struct {
	Env        string `yaml:"env"`
	Name       string `yaml:"name"`
	zap.Config `yaml:",inline"`
}

func (c *Config) BuildLogger(opts ...zap.Option) (*zap.Logger, error) {
	encoder := zap.NewDevelopmentEncoderConfig()
	if c.Logger.Env == "production" {
		encoder = zap.NewProductionEncoderConfig()
		if c.Logger.Sampling == nil {
			c.Logger.Sampling = &zap.SamplingConfig{
				Initial:    100,
				Thereafter: 100,
			}
		}
	}
	if len(c.Logger.OutputPaths) == 0 {
		c.Logger.OutputPaths = []string{"stderr"}
	}
	if len(c.Logger.ErrorOutputPaths) == 0 {
		c.Logger.OutputPaths = []string{"stderr"}
	}
	c.Logger.EncoderConfig = encoder
	logger, err := c.Logger.Build(opts...)
	if err != nil {
		return nil, err
	}
	if len(c.Logger.Name) > 0 {
		logger = logger.Named(c.Logger.Name)
	}
	return logger, nil
}
