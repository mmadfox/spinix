package cluster

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/zap"

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

func (s *server) VNodeStats(_ context.Context, req *pb.VNodeStatsRequest) (*pb.VNodeStatsResponse, error) {
	return &pb.VNodeStatsResponse{}, nil
}

func (s *server) Synchronize(_ context.Context, req *pb.SynchronizeRequest) (*pb.SynchronizeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	coordinator, err := s.findCoordinatorInfo(req)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Routes table has been pushed by",
		zap.String("coordinator", coordinator.Addr()))

	for i := 0; i < len(req.Routes); i++ {
		route := req.Routes[i]
		s.coordinator.SyncVNodeOwnersFromRoute(route)
	}

	s.coordinator.markBootstrapped()

	return s.makeSynchronizeResponse(), nil
}

func (s *server) findCoordinatorInfo(req *pb.SynchronizeRequest) (nodeInfo, error) {
	ni, err := s.coordinator.FindNodeInfoByID(req.CoordinatorId)
	if err != nil {
		return nodeInfo{}, status.Errorf(codes.NotFound, "node %d not found",
			req.CoordinatorId)
	}
	current, err := s.coordinator.Owner()
	if err != nil {
		return nodeInfo{}, status.Error(codes.NotFound, "coordinator node not found")
	}
	if !compareNodeByID(ni, current) {
		return nodeInfo{}, status.Errorf(codes.InvalidArgument, "unrecognized cluster coordinator: %s: %s",
			ni, current)
	}
	if s.coordinator.VNodes() != len(req.Routes) {
		return nodeInfo{}, status.Errorf(codes.InvalidArgument, "invalid vnodes count: got %d, expected: %d",
			len(req.Routes), s.coordinator.VNodes())
	}
	return current, nil
}

func (s *server) makeSynchronizeResponse() *pb.SynchronizeResponse {
	resp := &pb.SynchronizeResponse{}
	for i := 0; i < s.coordinator.VNodes(); i++ {
		nid := uint64(i)
		pVNode := s.coordinator.pVNodeList.ByID(nid)
		if !pVNode.NoData() {
			resp.ReportForPrimaryList = append(resp.ReportForPrimaryList)
		}
		sVNode := s.coordinator.sVNodeList.ByID(nid)
		if !sVNode.NoData() {
			resp.ReportForSecondaryList = append(resp.ReportForSecondaryList, nid)
		}
	}
	return resp
}
