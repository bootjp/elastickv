package adapter

import (
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithInternalCASForwardRouting binds CAS forwarding to the group served by
// this Internal endpoint and the same key router used by public dispatch.
func WithInternalCASForwardRouting(groupID uint64, resolve func([]byte) (uint64, error)) InternalOption {
	return func(i *Internal) {
		i.casForwardGroup = groupID
		i.casForwardRoute = resolve
	}
}

func (i *Internal) preflightForwardCAS(req *pb.ForwardRequest) error {
	reqs := req.GetRequests()
	if !kv.HasCASMutation(reqs) {
		return nil
	}
	mut, err := kv.ForwardCASMutation(reqs)
	if err != nil || !req.GetIsTxn() {
		return errors.WithStack(status.Error(codes.InvalidArgument, "invalid forwarded compare-and-swap"))
	}
	if i.casForwardRoute != nil {
		group, err := i.casForwardRoute(mut.Key)
		if errors.Is(err, kv.ErrVersionCASUnavailable) {
			return errors.WithStack(kv.VersionCASForwardUnavailable(err))
		}
		if err != nil || group != i.casForwardGroup {
			return errors.WithStack(status.Error(codes.FailedPrecondition, "compare-and-swap key is not owned by this group"))
		}
	}
	if len(reqs[0].GetWriteFenceBypassKeys()) != 0 && !i.canBypassCASWriteFence(mut.Key) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "compare-and-swap write fence bypass is unauthorized"))
	}
	return nil
}

func (i *Internal) canBypassCASWriteFence(key []byte) bool {
	if i.migrationExportResolver == nil || i.casForwardGroup == 0 {
		return false
	}
	owner, ok := i.migrationExportResolver.ResolveGroup(key)
	return ok && owner == i.casForwardGroup
}
