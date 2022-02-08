package tracker

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mmadfox/geojson"

	pb "github.com/mmadfox/spinix/gen/proto/go/api/v1"
	"google.golang.org/grpc"
)

type Server struct {
	pb.ApiServiceServer
	svc Service
}

func NewServer(grpcServer *grpc.Server, svc Service) *Server {
	srv := &Server{
		svc: svc,
	}
	pb.RegisterApiServiceServer(grpcServer, srv)
	return srv
}

func (s *Server) Add(ctx context.Context, req *pb.AddRequest) (*pb.AddResponse, error) {
	object := GeoJSON{
		ObjectID: req.GetObjectId(),
		Index:    uint2index(req.GetIndex()),
		LayerID:  req.GetLayerId(),
	}
	data, err := geojson.Parse(req.Data, geojson.DefaultParseOptions)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	object.Data = data
	index, err := s.svc.Add(ctx, object)
	if err != nil {
		return nil, err
	}
	return &pb.AddResponse{
		Index: index2uint(index),
	}, nil
}

func (s *Server) Remove(ctx context.Context, req *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	return nil, nil
}

func (s *Server) Detect(ctx context.Context, req *pb.DetectRequest) (*pb.DetectResponse, error) {
	return nil, nil
}
