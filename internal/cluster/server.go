package cluster

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
)

type server struct {
	pb.ClusterServiceServer
}

func newServer(grpcServer *grpc.Server) *server {
	srv := &server{}
	pb.RegisterClusterServiceServer(grpcServer, srv)
	return srv
}

func (s *server) SyncNode(ctx context.Context, req *pb.SyncNodeRequest) (*pb.SyncNodeResponse, error) {
	return nil, nil
}
