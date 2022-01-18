package cluster

import (
	"sync"

	"go.uber.org/zap"
)

type coordinator struct {
	mu          sync.RWMutex
	client      *pool
	nodeManager *nodeman
	logger      *zap.Logger
}

func (c *coordinator) Synchronize() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.nodeManager.IsCoordinator() {
		return
	}
}
