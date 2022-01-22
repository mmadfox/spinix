package cluster

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"golang.org/x/sync/semaphore"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"

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
	pVNodeList   *vnodeList
	sVNodeList   *vnodeList
}

func (c *coordinator) Bootstrap() error {
	return nil
}

func (c *coordinator) SyncVNode(r *pb.Route) {
	c.pVNodeList.ByID(r.VnodeId).SetOwners(r.GetPrimary())
	c.sVNodeList.ByID(r.VnodeId).SetOwners(r.GetSecondary())
}

func (c *coordinator) UpdateNumNodes() {
	c.router.SetNumNodes(c.nodeManager.NumNodes())
}

func (c *coordinator) FindNodeByID(id uint64) (nodeInfo, error) {
	return c.nodeManager.FindNodeByID(id)
}

func (c *coordinator) NodeInfo() (nodeInfo, error) {
	return c.nodeManager.Coordinator()
}

func (c *coordinator) VNodes() int {
	return c.router.NumVNodes()
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
	c.mu.Lock()
	defer c.mu.Unlock()

	c.UpdateNumNodes()
	if !c.nodeManager.IsCoordinator() {
		return
	}
	c.updateRoutersOnCluster()
}

func (c *coordinator) hasShutdown() bool {
	return atomic.LoadInt32(&c.shutdown) == 1
}

func (c *coordinator) markBootstrapped() {
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

func (c *coordinator) updateRoutersOnCluster() {
	newRoutes := c.makeRoutes()
	c.router.SetRoutes(newRoutes)
	if err := c.runWorkerPoolFor(newRoutes); err != nil {
		c.logger.Error("Update routers on cluster", zap.Error(err))
	}
}

func (c *coordinator) makeRoutes() []*pb.Route {
	routes := make([]*pb.Route, 0, 4)
	c.router.EachVNode(func(id uint64, addr string) bool {
		route := &pb.Route{
			VnodeId:   id,
			Primary:   c.makePrimaryVNodes(id),
			Secondary: c.makeSecondaryVNodes(id),
		}
		routes = append(routes, route)
		return true
	})
	return routes
}

func (c *coordinator) runWorkerPoolFor(routes []*pb.Route) error {
	var group errgroup.Group
	sem := semaphore.NewWeighted(int64(runtime.NumCPU()))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	req := &pb.SynchronizeRequest{
		CoordinatorId: c.nodeManager.Owner().ID(),
		Routes:        routes,
	}
	c.router.EachNode(func(ni nodeInfo) {
		addr := ni.Addr()
		group.Go(func() error {
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			return c.updateRouterOnNode(ctx, addr, req)
		})
	})
	return group.Wait()
}

func (c *coordinator) updateRouterOnNode(ctx context.Context, addr string, req *pb.SynchronizeRequest) error {
	client, cleanup, err := c.client.NewClient(ctx, addr)
	if err != nil {
		return err
	}
	defer cleanup()
	_, err = client.Synchronize(ctx, req)
	return err
}

func (c *coordinator) makePrimaryVNodes(vnode uint64) []*pb.NodeInfo {
	// TODO:
	return []*pb.NodeInfo{}
}

func (c *coordinator) makeSecondaryVNodes(vnode uint64) []*pb.NodeInfo {
	// TODO:
	return []*pb.NodeInfo{}
}
