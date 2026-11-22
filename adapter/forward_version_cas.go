package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *Internal) ForwardCompareAndSwap(context.Context, *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	return nil, errors.WithStack(kv.VersionCASForwardUnavailable(kv.ErrVersionCASUnavailable))
}

// The receiver must support Raft-ordered routing before it can accept this
// request. An older server has no V2 method and cannot submit the write.
func (i *Internal) ForwardCompareAndSwapV2(ctx context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if !kv.HasCASMutation(req.GetRequests()) {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "expected compare-and-swap"))
	}
	return i.forwardRequests(ctx, req)
}
