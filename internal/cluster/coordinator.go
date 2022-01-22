package cluster

import (
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type coordinator struct {
	mu           sync.RWMutex
	client       *pool
	nodeManager  *nodeman
	router       *router
	closeCh      chan struct{}
	logger       *zap.Logger
	pushInterval time.Duration
	shutdown     int32
	owner        nodeInfo
	bootstrapped int32
}

func (c *coordinator) Bootstrap() error {
	return nil
}

func (c *coordinator) UpdateNumNodes() {
	c.router.UpdateNumNodes(c.nodeManager.NumNodes())
}

func (c *coordinator) Run() error {
	if c.nodeManager.IsCoordinator() {
		if err := c.Bootstrap(); err != nil {
			return err
		}
	}
	go c.updateChangeStateByPushInterval()
	return nil
}

func (c *coordinator) Shutdown() error {
	if c.hasShutdown() {
		return nil
	}
	atomic.StoreInt32(&c.shutdown, 1)
	close(c.closeCh)
	return nil
}

func (c *coordinator) Synchronize() {
	c.UpdateNumNodes()

	c.logger.Info("Synchronization of the coordinator",
		zap.Int32("numNodes", c.router.NumNodes()),
		zap.String("nodes", c.router.String()))

	if !c.nodeManager.IsCoordinator() {
		return
	}

	c.logger.Info("Start synchronize coordinator")
}

func (c *coordinator) hasShutdown() bool {
	return atomic.LoadInt32(&c.shutdown) == 1
}

func (c *coordinator) setBootstrapped() {
	atomic.StoreInt32(&c.bootstrapped, 1)
}

func (c *coordinator) hasBootstrapped() bool {
	return atomic.LoadInt32(&c.bootstrapped) == 1
}

func (c *coordinator) updateChangeStateByPushInterval() {
	ticker := time.NewTicker(c.pushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.Synchronize()
		}
	}
}

func (c *coordinator) updateRoutersOnCluster() error {
	return nil
}
