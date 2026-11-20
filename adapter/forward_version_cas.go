package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Using a distinct method keeps an old leader from appending a CAS in its
// legacy log format before discovering the unsupported mutation during apply.
func (i *Internal) ForwardCompareAndSwap(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if !kv.HasCASMutation(req.GetRequests()) {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "expected compare-and-swap"))
	}
	return i.forwardRequests(ctx, req)
}
