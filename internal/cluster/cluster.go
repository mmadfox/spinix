package cluster

import (
	"fmt"

	"github.com/sirupsen/logrus"

	h3geodist "github.com/mmadfox/go-h3geo-dist"

	"google.golang.org/grpc"

	"github.com/mmadfox/spinix/internal/transport"
)

type Cluster struct {
	opts        *Options
	nodeManager *nodeman
	router      *router
	client      *client
	balancer    *balancer
	logger      *logrus.Logger
}

func New(logger *logrus.Logger, opts *Options) (*Cluster, error) {
	if opts == nil {
		return nil, fmt.Errorf("cluster: options cannot be nil")
	}
	cluster := &Cluster{
		opts:   opts,
		logger: logger,
	}
	if err := setupClient(cluster); err != nil {
		return nil, err
	}
	if err := setupNodeManager(cluster); err != nil {
		return nil, err
	}
	if err := setupRouter(cluster); err != nil {
		return nil, err
	}
	if err := setupBalancer(cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (c *Cluster) Run() error {
	return c.nodeManager.ListenAndServe()
}

func (c *Cluster) Shutdown() error {
	return c.nodeManager.Shutdown()
}

func (c *Cluster) handleNodeJoin(ni *nodeInfo) {
	if err := c.router.AddNode(ni); err != nil {
		c.logger.Errorf("Node %s join error: %v", ni.Addr(), err)
	} else {
		c.logger.Infof("Node joined: %s", ni)
	}
}

func (c *Cluster) handleNodeLeave(ni *nodeInfo) {
	c.router.RemoveNode(ni)
	c.logger.Infof("Node leaved: %s", ni)
}

func (c *Cluster) handleNodeUpdate(ni *nodeInfo) {
	if err := c.router.UpdateNode(ni); err != nil {
		c.logger.Errorf("Node %s update error: %v", ni.Addr(), err)
	} else {
		c.logger.Infof("Node updated: %s", ni)
	}
}

func (c *Cluster) handleChangeState() {

}

func setupRouter(c *Cluster) error {
	h3dist, err := h3geodist.New(c.opts.H3DistLevel,
		h3geodist.WithVNodes(c.opts.H3DistVNodes),
		h3geodist.WithReplicationFactor(c.opts.H3DistReplicas),
	)
	if err != nil {
		return err
	}
	c.router = newRouter(h3dist, c.client)
	c.nodeManager.OnJoinFunc(c.handleNodeJoin)
	c.nodeManager.OnLeaveFunc(c.handleNodeLeave)
	c.nodeManager.OnUpdateFunc(c.handleNodeUpdate)
	c.nodeManager.OnChangeFunc(c.handleChangeState)
	return nil
}

func setupBalancer(c *Cluster) error {
	return nil
}

func setupNodeManager(c *Cluster) error {
	owner := makeNodeInfo(c.opts.GRPCServerAddr, c.opts.GRPCServerPort)
	nodeManagerConf := toMemberlistConf(c.opts)
	nodeManager, err := nodemanFromMemberlistConfig(owner, nodeManagerConf)
	if err != nil {
		return err
	}
	c.nodeManager = nodeManager
	return nil
}

func setupClient(c *Cluster) error {
	pool, err := transport.NewPool(&transport.PoolOptions{
		IdleTimeout:     c.opts.GRPCClientIdleTimeout,
		MaxLifeDuration: c.opts.GRPCClientMaxLifeDuration,
		Init:            c.opts.GRPCClientInitPoolCount,
		Capacity:        c.opts.GRPCClientPoolCapacity,
		NewConn: func(addr string) (*grpc.ClientConn, error) {
			return grpc.Dial(addr, c.opts.GRPCClientDialOpts...)
		},
	})
	if err != nil {
		return err
	}
	c.client = newClient(pool)
	return nil
}
