package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithInternalCASForwardRouting binds CAS forwarding to the group served by
// this endpoint and checks that group's applied catalog.
func WithInternalCASForwardRouting(groupID uint64, check func(context.Context, uint64, []byte) error) InternalOption {
	return func(i *Internal) {
		i.casForwardGroup = groupID
		i.casForwardRoute = check
	}
}

func (i *Internal) preflightForwardCAS(ctx context.Context, req *pb.ForwardRequest) error {
	reqs := req.GetRequests()
	if !kv.HasCASMutation(reqs) {
		return nil
	}
	mut, err := kv.ForwardCASMutation(reqs)
	if err != nil || !req.GetIsTxn() {
		return errors.WithStack(status.Error(codes.InvalidArgument, "invalid forwarded compare-and-swap"))
	}
	if i.casForwardGroup != reqs[0].GetVersionCasGroupId() || i.casForwardRoute == nil {
		return errors.WithStack(kv.VersionCASForwardUnavailable(kv.ErrVersionCASUnavailable))
	}
	if len(reqs[0].GetWriteFenceBypassKeys()) != 0 {
		return errors.WithStack(status.Error(codes.PermissionDenied, "compare-and-swap write fence bypass is unauthorized"))
	}
	if err := i.casForwardRoute(ctx, i.casForwardGroup, mut.Key); err != nil {
		return errors.WithStack(kv.VersionCASForwardUnavailable(err))
	}
	return nil
}
