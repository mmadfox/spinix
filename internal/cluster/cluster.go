package cluster

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	h3geodist "github.com/mmadfox/go-h3geo-dist"

	"google.golang.org/grpc"

	"github.com/mmadfox/spinix/internal/transport"
)

type Cluster struct {
	wg          sync.WaitGroup
	opts        *Options
	nodeManager *nodeman
	router      *router
	client      *client
	server      *server
	balancer    *balancer
	logger      *zap.Logger
	coordinator *coordinator
}

func New(grpcServer *grpc.Server, logger *zap.Logger, opts *Options) (*Cluster, error) {
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
	if err := setupServer(cluster, grpcServer); err != nil {
		return nil, err
	}

	setupCoordinator(cluster)

	return cluster, nil
}

func (c *Cluster) Run() (err error) {
	if err = c.nodeManager.ListenAndServe(); err != nil {
		return err
	}
	// TODO:
	for i := 0; i < 3; i++ {
		time.Sleep(50 * time.Millisecond)
		if _, err = c.nodeManager.Join(c.opts.Peers); err == nil {
			break
		}
	}
	c.wg.Add(1)
	c.wg.Wait()
	return
}

func (c *Cluster) Shutdown() error {
	defer c.wg.Done()
	return c.nodeManager.Shutdown()
}

func (c *Cluster) handleNodeJoin(ni *nodeInfo) {
	if err := c.router.AddNode(ni); err != nil {
		c.logger.Error("Node join error",
			zap.String("host", ni.Addr()), zap.Error(err))
	} else {
		c.logger.Info("Node joined", zap.String("host", ni.Addr()))
	}
}

func (c *Cluster) handleNodeLeave(ni *nodeInfo) {
	c.router.RemoveNode(ni)
	c.client.Close(ni.Addr())
	c.logger.Info("Node leaved", zap.String("host", ni.Addr()))
}

func (c *Cluster) handleNodeUpdate(ni *nodeInfo) {
	if err := c.router.UpdateNode(ni); err != nil {
		c.logger.Error("Node update",
			zap.String("host", ni.Addr()), zap.Error(err))
	} else {
		c.logger.Info("Node updated", zap.String("host", ni.Addr()))
	}
}

func (c *Cluster) handleChangeState() {
	c.coordinator.Synchronize()
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

func setupServer(c *Cluster, grpcServer *grpc.Server) error {
	c.server = newServer(grpcServer)
	return nil
}

func setupCoordinator(c *Cluster) {
	c.coordinator = &coordinator{
		client:      c.client,
		logger:      c.logger,
		nodeManager: c.nodeManager,
	}
}

func setupBalancer(c *Cluster) error {
	return nil
}

func setupNodeManager(c *Cluster) error {
	owner := makeNodeInfo(c.opts.GRPCServerAddr, c.opts.GRPCServerPort)
	nodeManagerConf := toMemberlistConf(c.opts)
	nodeManagerConf.Logger = zap.NewStdLog(c.logger)
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
			return grpc.Dial(addr)
		},
	})
	if err != nil {
		return err
	}
	c.client = newClient(pool)
	return nil
}
