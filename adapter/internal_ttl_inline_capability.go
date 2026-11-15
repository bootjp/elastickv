package adapter

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithInternalTTLInlineCapabilityProbe installs the protected peer proof handler.
func WithInternalTTLInlineCapabilityProbe(probe func(context.Context, *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error)) InternalOption {
	return func(i *Internal) { i.ttlInlineCapabilityProbe = probe }
}

func (i *Internal) GetTTLInlineCapability(ctx context.Context, req *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
	if i.ttlInlineCapabilityProbe == nil {
		return nil, errors.WithStack(status.Error(codes.Unavailable, "TTL-inline capability probe unavailable"))
	}
	return i.ttlInlineCapabilityProbe(ctx, req)
}
