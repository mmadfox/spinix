package cluster

import (
	"context"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
)

type Server struct {
	pb.ClusterServiceServer
	cluster *Cluster
}

func (s *Server) SyncNode(ctx context.Context, req *pb.SyncNodeRequest) (*pb.SyncNodeResponse, error) {
	return nil, nil
}
