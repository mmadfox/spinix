package cluster

import (
	"context"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"github.com/mmadfox/spinix/internal/transport"
)

type client struct {
	pool *transport.Pool
}

func newClient(pool *transport.Pool) *client {
	return &client{pool: pool}
}

func (c *client) Close(addr string) {
	c.pool.ClosePool(addr)
}

func (c *client) SyncNode(ctx context.Context, addr string, req *pb.SyncNodeRequest) (*pb.SyncNodeResponse, error) {
	conn, err := c.pool.Conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := pb.NewClusterServiceClient(conn.ClientConn)
	return cli.SyncNode(ctx, req)
}
