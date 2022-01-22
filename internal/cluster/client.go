package cluster

import (
	"context"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"github.com/mmadfox/spinix/internal/transport"
)

type pool struct {
	pool *transport.Pool
}

func newPool(p *transport.Pool) *pool {
	return &pool{pool: p}
}

func (c *pool) Close(addr string) {
	c.pool.ClosePool(addr)
}

func (c *pool) NewClient(ctx context.Context, addr string) (pb.ClusterServiceClient, error) {
	conn, err := c.pool.Conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	return pb.NewClusterServiceClient(conn), nil
}
