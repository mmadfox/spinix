package cluster

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/hashicorp/go-multierror"

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
	client      *pool
	server      *server
	balancer    *balancer
	logger      *zap.Logger
	pVNodeList  *vnodeList
	sVNodeList  *vnodeList
	coordinator *coordinator
	h3dist      *h3geodist.Distributed
}

func New(grpcServer *grpc.Server, logger *zap.Logger, opts *Options) (*Cluster, error) {
	if opts == nil {
		return nil, fmt.Errorf("cluster: options cannot be nil")
	}
	cluster := &Cluster{
		opts:   opts,
		logger: logger,
	}
	if err := setupH3GeoDist(cluster); err != nil {
		return nil, err
	}

	cluster.router = newRouter(cluster.h3dist, logger)

	if err := setupClient(cluster); err != nil {
		return nil, err
	}
	if err := setupNodeManager(cluster); err != nil {
		return nil, err
	}
	if err := setupBalancer(cluster); err != nil {
		return nil, err
	}

	cluster.pVNodeList = newVNodeList(cluster.h3dist.VNodes(), Primary)
	cluster.sVNodeList = newVNodeList(cluster.h3dist.VNodes(), Secondary)

	cluster.coordinator = &coordinator{
		client:           cluster.client,
		logger:           logger,
		nodeManager:      cluster.nodeManager,
		router:           cluster.router,
		closeCh:          make(chan struct{}),
		pushInterval:     opts.CoordinatorPushInterval,
		pVNodeList:       cluster.pVNodeList,
		sVNodeList:       cluster.sVNodeList,
		bootstrapTimeout: opts.BootstrapTimeout,
	}
	cluster.server = newServer(grpcServer, cluster.coordinator, logger)
	return cluster, nil
}

func (c *Cluster) Run() (err error) {
	c.wg.Add(1)

	c.nodeManager.OnJoinFunc(c.handleNodeJoin)
	c.nodeManager.OnLeaveFunc(c.handleNodeLeave)
	c.nodeManager.OnUpdateFunc(c.handleNodeUpdate)
	c.nodeManager.OnChangeFunc(c.handleChangeState)

	if err = c.nodeManager.ListenAndServe(); err != nil {
		return err
	}

	if err := c.joinNodeToCluster(); err != nil {
		return err
	}

	if err := c.router.AddNode(c.nodeManager.Owner()); err != nil {
		return err
	}

	c.coordinator.SyncNumNodes()

	if err := c.coordinator.Run(); err != nil {
		return err
	}

	c.logger.Info("Cluster running",
		zap.String("node", c.nodeManager.Owner().String()),
		zap.Bool("isCoordinator", c.nodeManager.IsCoordinator()),
	)

	c.wg.Wait()
	return
}

func (c *Cluster) Shutdown() (err error) {
	defer c.wg.Done()
	if er := c.nodeManager.Shutdown(); er != nil {
		err = multierror.Append(err, err)
	}
	if er := c.balancer.Shutdown(); er != nil {
		err = multierror.Append(err, er)
	}
	if er := c.coordinator.Shutdown(); er != nil {
		err = multierror.Append(err, er)
	}
	return
}

func (c *Cluster) handleNodeJoin(ni nodeInfo) {
	if err := c.router.AddNode(ni); err != nil {
		c.logger.Error("Node join error",
			zap.String("host", ni.Addr()), zap.Error(err))
		return
	}
	c.logger.Info("Node joined", zap.String("host", ni.Addr()))
}

func (c *Cluster) joinNodeToCluster() (err error) {
	var joinOk bool
	for i := 0; i < c.opts.MaxJoinAttempts; i++ {
		if c.nodeManager.hasShutdown() {
			break
		}
		if _, err = c.nodeManager.Join(c.opts.Peers); err == nil {
			joinOk = true
			break
		}
		if err != nil {
			c.logger.Error("Node join error", zap.Error(err))
		}
		c.logger.Info("Waiting for next join",
			zap.Int("maxJoinAttempts", c.opts.MaxJoinAttempts),
			zap.Int("currentJoinAttempts", i),
			zap.Duration("joinRetryInterval", c.opts.JoinRetryInterval),
		)
		<-time.After(c.opts.JoinRetryInterval)
	}
	if joinOk && err == nil {
		err = c.nodeManager.ValidateOwner()
	}
	return
}

func (c *Cluster) handleNodeLeave(ni nodeInfo) {
	c.router.RemoveNode(ni)
	c.client.Close(ni.Addr())
	c.logger.Info("Node leaved", zap.String("host", ni.Addr()))
}

func (c *Cluster) handleNodeUpdate(ni nodeInfo) {
	if err := c.router.UpdateNode(ni); err != nil {
		c.logger.Error("Node update",
			zap.String("host", ni.Addr()), zap.Error(err))
		return
	}
	c.logger.Info("Node updated", zap.String("host", ni.Addr()))
}

func (c *Cluster) handleChangeState() {
	c.coordinator.Synchronize()
}

func setupH3GeoDist(c *Cluster) error {
	h3dist, err := h3geodist.New(c.opts.H3DistLevel,
		h3geodist.WithVNodes(c.opts.H3DistVNodes),
		h3geodist.WithReplicationFactor(c.opts.H3DistReplicas),
	)
	if err != nil {
		return err
	}
	c.h3dist = h3dist
	return nil
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
			var opts []grpc.DialOption
			if len(c.opts.GRPCClientDialOpts) > 0 {
				opts = append(opts, c.opts.GRPCClientDialOpts...)
			} else {
				// default options
				opts = append(opts, grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				))
			}
			return grpc.Dial(addr, opts...)
		},
	})
	if err != nil {
		return err
	}
	c.client = newPool(pool)
	return nil
}
