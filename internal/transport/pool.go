package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	grpcpool "github.com/processout/grpc-go-pool"
)

type PoolOptions struct {
	NewConn         func(addr string) (*grpc.ClientConn, error)
	IdleTimeout     time.Duration
	MaxLifeDuration time.Duration
	Init            int
	Capacity        int
}

type Pool struct {
	mu    sync.RWMutex
	opt   *PoolOptions
	pools map[string]*grpcpool.Pool
}

func NewPool(opts *PoolOptions) (*Pool, error) {
	if opts == nil {
		return nil, fmt.Errorf("transport/pool: options cannot be nil")
	}
	return &Pool{
		opt:   opts,
		pools: make(map[string]*grpcpool.Pool),
	}, nil
}

func (c *Pool) Conn(ctx context.Context, addr string) (*grpcpool.ClientConn, error) {
	pool, err := c.getOrCreatePoolByAddr(addr)
	if err != nil {
		return nil, err
	}
	return pool.Get(ctx)
}

func (c *Pool) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pool := range c.pools {
		pool.Close()
	}
	c.pools = make(map[string]*grpcpool.Pool)
}

func (c *Pool) ClosePool(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pool, found := c.pools[addr]
	if !found {
		return
	}
	if !pool.IsClosed() {
		pool.Close()
	}
	delete(c.pools, addr)
	return
}

func (c *Pool) getOrCreatePoolByAddr(addr string) (*grpcpool.Pool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pool, found := c.pools[addr]
	if found {
		return pool, nil
	}
	factory := func() (*grpc.ClientConn, error) { return c.opt.NewConn(addr) }
	pool, err := grpcpool.New(
		factory,
		c.opt.Init,
		c.opt.Capacity,
		c.opt.IdleTimeout,
		c.opt.MaxLifeDuration,
	)
	if err != nil {
		return nil, err
	}
	c.pools[addr] = pool
	return pool, nil
}
