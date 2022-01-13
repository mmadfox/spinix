package cluster

import (
	"context"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"github.com/mmadfox/spinix/internal/transport"
)

type Client struct {
	pool *transport.Pool
}

func NewClient(pool *transport.Pool) *Client {
	return &Client{pool: pool}
}

func (c *Client) SyncNode(ctx context.Context, addr string, req *pb.SyncNodeRequest) (*pb.SyncNodeResponse, error) {
	conn, err := c.pool.Conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := pb.NewClusterServiceClient(conn.ClientConn)
	return cli.SyncNode(ctx, req)
}
