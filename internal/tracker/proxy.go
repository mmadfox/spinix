package tracker

import (
	"context"

	grpcpool "github.com/processout/grpc-go-pool"

	pb "github.com/mmadfox/spinix/gen/proto/go/api/v1"

	"github.com/mmadfox/spinix/internal/transport"
	"github.com/uber/h3-go/v3"
)

type Proxy interface {
	NewClient(ctx context.Context, addr string) (Service, error)
}

type proxy struct {
	pool *transport.Pool
}

func newProxy(p *transport.Pool) *proxy {
	return &proxy{pool: p}
}

func (p *proxy) NewClient(ctx context.Context, addr string) (Service, error) {
	conn, err := p.pool.Conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &client{
		conn:             conn,
		ApiServiceClient: pb.NewApiServiceClient(conn),
	}, nil
}

type client struct {
	pb.ApiServiceClient
	conn *grpcpool.ClientConn
}

func (c *client) Add(ctx context.Context, object GeoJSON) ([]h3.H3Index, error) {
	defer c.conn.Close()
	req := &pb.AddRequest{
		ObjectId: object.ObjectID,
		Index:    index2uint(object.Index),
		LayerId:  object.LayerID,
		Data:     object.Data.JSON(),
	}
	resp, err := c.ApiServiceClient.Add(ctx, req)
	if err != nil {
		return nil, err
	}
	return uint2index(resp.Index), nil
}

func (c *client) Remove(ctx context.Context, objectID uint64, index []h3.H3Index) error {
	return nil
}

func (c *client) Detect(ctx context.Context) (events []Event, ok bool, err error) {
	return
}

func index2uint(index []h3.H3Index) []uint64 {
	ids := make([]uint64, len(index))
	for i := 0; i < len(index); i++ {
		ids[i] = uint64(index[i])
	}
	return ids
}

func uint2index(ids []uint64) []h3.H3Index {
	index := make([]h3.H3Index, len(ids))
	for i := 0; i < len(ids); i++ {
		index[i] = h3.H3Index(index[i])
	}
	return index
}
