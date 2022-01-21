package cluster

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"github.com/mmadfox/spinix/internal/transport"
	grpcpool "github.com/processout/grpc-go-pool"
)

type client struct {
	pb.ClusterServiceClient
	conn *grpcpool.ClientConn
}

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
	return client{conn: conn}, nil
}

func (c client) Close() error {
	return c.conn.Close()
}

func (c client) VNodeStats(ctx context.Context, in *pb.VNodeStatsRequest, opts ...grpc.CallOption) (*pb.VNodeStatsResponse, error) {
	return &pb.VNodeStatsResponse{}, nil
}

func (c client) Synchronize(ctx context.Context, in *pb.SynchronizeRequest, opts ...grpc.CallOption) (*pb.SynchronizeResponse, error) {
	return &pb.SynchronizeResponse{}, nil
}
