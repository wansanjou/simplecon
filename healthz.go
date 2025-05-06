package simplecon

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type grpcHandler struct{}

func NewGrpc() grpc_health_v1.HealthServer {
	return &grpcHandler{}
}

func (h *grpcHandler) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (h *grpcHandler) Watch(in *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	// Example of how to register both methods but only implement the Check method.
	return status.Error(codes.Unimplemented, "unimplemented")
}
