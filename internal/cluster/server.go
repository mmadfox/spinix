package cluster

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"google.golang.org/grpc/codes"

	"google.golang.org/grpc/status"

	"google.golang.org/grpc"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
)

type server struct {
	mu          sync.RWMutex
	coordinator *coordinator
	logger      *zap.Logger
	pb.ClusterServiceServer
}

func newServer(grpcServer *grpc.Server, c *coordinator, logger *zap.Logger) *server {
	srv := &server{coordinator: c, logger: logger}
	pb.RegisterClusterServiceServer(grpcServer, srv)
	return srv
}

func (s *server) VNodeStats(ctx context.Context, req *pb.VNodeStatsRequest) (*pb.VNodeStatsResponse, error) {
	return nil, nil
}

func (s *server) Synchronize(_ context.Context, req *pb.SynchronizeRequest) (*pb.SynchronizeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ni, err := s.coordinator.FindNodeByID(req.CoordinatorId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "node %d not found",
			req.CoordinatorId)
	}

	s.logger.Info("Routes table has been pushed by",
		zap.String("coordinator", ni.Addr()))

	current, err := s.coordinator.NodeInfo()
	if err != nil {
		return nil, status.Error(codes.NotFound, "coordinator node not found")
	}
	if !compareNodeByID(ni, current) {
		return nil, status.Errorf(codes.InvalidArgument, "unrecognized cluster coordinator: %s: %s",
			ni, current)
	}
	if s.coordinator.VNodes() != len(req.Routes) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid vnodes count: got %d, expected: %d",
			len(req.Routes), s.coordinator.VNodes())
	}

	for i := 0; i < len(req.Routes); i++ {
		route := req.Routes[i]
		s.coordinator.SyncVNode(route)
	}

	s.coordinator.markBootstrapped()

	return &pb.SynchronizeResponse{}, nil
}
