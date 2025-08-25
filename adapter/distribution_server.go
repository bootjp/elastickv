package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
)

// DistributionServer serves distribution related gRPC APIs.
type DistributionServer struct {
	engine *distribution.Engine
	pb.UnimplementedDistributionServer
}

// NewDistributionServer creates a new server.
func NewDistributionServer(e *distribution.Engine) *DistributionServer {
	return &DistributionServer{engine: e}
}

// UpdateRoute allows updating route information.
func (s *DistributionServer) UpdateRoute(start []byte, group uint64) {
	s.engine.UpdateRoute(start, group)
}

// GetRoute returns route for a key.
func (s *DistributionServer) GetRoute(ctx context.Context, req *pb.GetRouteRequest) (*pb.GetRouteResponse, error) {
	r, ok := s.engine.GetRoute(req.Key)
	if !ok {
		return &pb.GetRouteResponse{}, nil
	}
	return &pb.GetRouteResponse{
		Start:       r.Start,
		RaftGroupId: r.GroupID,
	}, nil
}

// GetTimestamp returns monotonically increasing timestamp.
func (s *DistributionServer) GetTimestamp(ctx context.Context, req *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	ts := s.engine.NextTimestamp()
	return &pb.GetTimestampResponse{Timestamp: ts}, nil
}
