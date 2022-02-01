// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.19.1
// source: cluster/v1/service.proto

package clusterv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ClusterServiceClient is the client API for ClusterService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClusterServiceClient interface {
	VNodeStats(ctx context.Context, in *VNodeStatsRequest, opts ...grpc.CallOption) (*VNodeStatsResponse, error)
	Synchronize(ctx context.Context, in *SynchronizeRequest, opts ...grpc.CallOption) (*SynchronizeResponse, error)
}

type clusterServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClusterServiceClient(cc grpc.ClientConnInterface) ClusterServiceClient {
	return &clusterServiceClient{cc}
}

func (c *clusterServiceClient) VNodeStats(ctx context.Context, in *VNodeStatsRequest, opts ...grpc.CallOption) (*VNodeStatsResponse, error) {
	out := new(VNodeStatsResponse)
	err := c.cc.Invoke(ctx, "/cluster.v1.ClusterService/VNodeStats", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterServiceClient) Synchronize(ctx context.Context, in *SynchronizeRequest, opts ...grpc.CallOption) (*SynchronizeResponse, error) {
	out := new(SynchronizeResponse)
	err := c.cc.Invoke(ctx, "/cluster.v1.ClusterService/Synchronize", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClusterServiceServer is the server API for ClusterService service.
// All implementations should embed UnimplementedClusterServiceServer
// for forward compatibility
type ClusterServiceServer interface {
	VNodeStats(context.Context, *VNodeStatsRequest) (*VNodeStatsResponse, error)
	Synchronize(context.Context, *SynchronizeRequest) (*SynchronizeResponse, error)
}

// UnimplementedClusterServiceServer should be embedded to have forward compatible implementations.
type UnimplementedClusterServiceServer struct {
}

func (UnimplementedClusterServiceServer) VNodeStats(context.Context, *VNodeStatsRequest) (*VNodeStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VNodeStats not implemented")
}
func (UnimplementedClusterServiceServer) Synchronize(context.Context, *SynchronizeRequest) (*SynchronizeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Synchronize not implemented")
}

// UnsafeClusterServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClusterServiceServer will
// result in compilation errors.
type UnsafeClusterServiceServer interface {
	mustEmbedUnimplementedClusterServiceServer()
}

func RegisterClusterServiceServer(s grpc.ServiceRegistrar, srv ClusterServiceServer) {
	s.RegisterService(&ClusterService_ServiceDesc, srv)
}

func _ClusterService_VNodeStats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(VNodeStatsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServiceServer).VNodeStats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.v1.ClusterService/VNodeStats",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServiceServer).VNodeStats(ctx, req.(*VNodeStatsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ClusterService_Synchronize_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SynchronizeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServiceServer).Synchronize(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/cluster.v1.ClusterService/Synchronize",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServiceServer).Synchronize(ctx, req.(*SynchronizeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ClusterService_ServiceDesc is the grpc.ServiceDesc for ClusterService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClusterService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "cluster.v1.ClusterService",
	HandlerType: (*ClusterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "VNodeStats",
			Handler:    _ClusterService_VNodeStats_Handler,
		},
		{
			MethodName: "Synchronize",
			Handler:    _ClusterService_Synchronize_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cluster/v1/service.proto",
}
