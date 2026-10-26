package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// CoordinatorForwarder forwards Coordinator and every optional coordinator
// capability the adapters type-assert to Inner. Decorators that intercept a
// few calls (the startup gate in main, the keyviz label wrapper) embed it and
// override only those calls, so a capability they do not touch still reaches
// the wrapped coordinator instead of silently failing the adapters' type
// assertion.
//
// When Inner does not implement a capability, the forwarder falls back to the
// behaviour of a coordinator without it: no write floors, no group-keyed
// routing, no membership, and plain lease/linearizable reads.
type CoordinatorForwarder struct {
	Inner Coordinator
}

// ErrOperationGroupResolverUnavailable means the wrapped coordinator has no
// operation routing capability; a sharded filesystem must fail closed.
var ErrOperationGroupResolverUnavailable = errors.New("operation group resolver unavailable")

var (
	_ Coordinator                          = CoordinatorForwarder{}
	_ LeaseReadableCoordinator             = CoordinatorForwarder{}
	_ AllGroupsLeaseReadableCoordinator    = CoordinatorForwarder{}
	_ AllGroupsLeaseTimestampCoordinator   = CoordinatorForwarder{}
	_ GroupRoutableCoordinator             = CoordinatorForwarder{}
	_ GroupLeaderRoutableCoordinator       = CoordinatorForwarder{}
	_ LocalLeaderGroupsCoordinator         = CoordinatorForwarder{}
	_ RaftMembershipCoordinator            = CoordinatorForwarder{}
	_ TimestampAllocatorProvider           = CoordinatorForwarder{}
	_ ConfiguredTimestampAllocatorProvider = CoordinatorForwarder{}
	_ TimestampAllocator                   = CoordinatorForwarder{}
	_ TimestampAfterAllocator              = CoordinatorForwarder{}
	_ TSOPhaseDState                       = CoordinatorForwarder{}
	_ DurableTimestampValidator            = CoordinatorForwarder{}
	_ AppliedReadTimestampVoucher          = CoordinatorForwarder{}
	_ AppliedReadTimestampVoucherRevoker   = CoordinatorForwarder{}
	_ AppliedReadTimestampVoucherSupport   = CoordinatorForwarder{}
	_ MutationWriteGate                    = CoordinatorForwarder{}
	_ hlcLeaseRecoverer                    = CoordinatorForwarder{}
)

func (f CoordinatorForwarder) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	resp, err := f.Inner.Dispatch(ctx, reqs)
	return resp, errors.WithStack(err)
}

func (f CoordinatorForwarder) IsLeader() bool { return f.Inner.IsLeader() }

func (f CoordinatorForwarder) VerifyLeader(ctx context.Context) error {
	return errors.WithStack(f.Inner.VerifyLeader(ctx))
}

func (f CoordinatorForwarder) LinearizableRead(ctx context.Context) (uint64, error) {
	idx, err := f.Inner.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

func (f CoordinatorForwarder) LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error) {
	if lr, ok := f.Inner.(interface {
		LinearizableReadForKey(context.Context, []byte) (uint64, error)
	}); ok {
		idx, err := lr.LinearizableReadForKey(ctx, key)
		return idx, errors.WithStack(err)
	}
	return f.LinearizableRead(ctx)
}

func (f CoordinatorForwarder) RaftLeader() string { return f.Inner.RaftLeader() }

func (f CoordinatorForwarder) IsLeaderForKey(key []byte) bool { return f.Inner.IsLeaderForKey(key) }

func (f CoordinatorForwarder) VerifyLeaderForKey(ctx context.Context, key []byte) error {
	return errors.WithStack(f.Inner.VerifyLeaderForKey(ctx, key))
}

func (f CoordinatorForwarder) RaftLeaderForKey(key []byte) string {
	return f.Inner.RaftLeaderForKey(key)
}

func (f CoordinatorForwarder) Clock() *HLC { return f.Inner.Clock() }

func (f CoordinatorForwarder) TimestampAllocator() TimestampAllocator {
	alloc, _ := f.innerTimestampAllocator()
	return alloc
}

func (f CoordinatorForwarder) ConfiguredTimestampAllocator() TimestampAllocator {
	if f.Inner == nil {
		return nil
	}
	alloc, _ := ConfiguredTimestampAllocatorThrough(f.Inner)
	return alloc
}

func (f CoordinatorForwarder) innerTimestampAllocator() (TimestampAllocator, bool) {
	if f.Inner == nil {
		return nil, false
	}
	return TimestampAllocatorThrough(f.Inner)
}

func (f CoordinatorForwarder) PhaseDActive() bool {
	alloc, ok := f.innerTimestampAllocator()
	if !ok {
		return false
	}
	phaseD, ok := alloc.(TSOPhaseDState)
	return ok && phaseD.PhaseDActive()
}

func (f CoordinatorForwarder) PhaseDRequired() bool {
	alloc, ok := f.innerTimestampAllocator()
	if !ok {
		return false
	}
	phaseD, ok := alloc.(TSOPhaseDState)
	return ok && phaseD.PhaseDRequired()
}

func (f CoordinatorForwarder) ValidateDurableTimestamp(ctx context.Context, timestamp uint64) error {
	alloc, ok := f.innerTimestampAllocator()
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	validator, ok := alloc.(DurableTimestampValidator)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return errors.WithStack(validator.ValidateDurableTimestamp(ctx, timestamp))
}

func (f CoordinatorForwarder) Next(ctx context.Context) (uint64, error) {
	if alloc, ok := f.Inner.(TimestampAllocator); ok {
		ts, err := alloc.Next(ctx)
		return ts, errors.WithStack(err)
	}
	ts, err := NextTimestampThrough(ctx, f.Inner, "allocate forwarded timestamp")
	return ts, errors.WithStack(err)
}

func (f CoordinatorForwarder) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	if after, ok := f.Inner.(TimestampAfterAllocator); ok {
		ts, err := after.NextAfter(ctx, min)
		return ts, errors.WithStack(err)
	}
	ts, err := NextTimestampAfterThrough(ctx, f.Inner, min, "allocate forwarded timestamp after observed ts")
	return ts, errors.WithStack(err)
}

func (f CoordinatorForwarder) RecoverHLCLease(ctx context.Context) error {
	if recoverer, ok := f.Inner.(hlcLeaseRecoverer); ok {
		return errors.WithStack(recoverer.RecoverHLCLease(ctx))
	}
	return errors.WithStack(errHLCLeaseRecoveryUnavailable)
}

func (f CoordinatorForwarder) VouchAppliedReadTimestamp(timestamp uint64, ref AppliedReadTimestampVoucherRef) error {
	voucher, ok := f.Inner.(AppliedReadTimestampVoucher)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return errors.WithStack(voucher.VouchAppliedReadTimestamp(timestamp, ref))
}

func (f CoordinatorForwarder) RevokeAppliedReadTimestamp(timestamp uint64, ref AppliedReadTimestampVoucherRef) {
	if revoker, ok := f.Inner.(AppliedReadTimestampVoucherRevoker); ok {
		revoker.RevokeAppliedReadTimestamp(timestamp, ref)
	}
}

func (f CoordinatorForwarder) SupportsAppliedReadTimestampVoucher() bool {
	if support, ok := f.Inner.(AppliedReadTimestampVoucherSupport); ok {
		return support.SupportsAppliedReadTimestampVoucher()
	}
	_, ok := f.Inner.(AppliedReadTimestampVoucher)
	return ok
}

func (f CoordinatorForwarder) LeaseRead(ctx context.Context) (uint64, error) {
	return LeaseReadThrough(f.Inner, ctx)
}

func (f CoordinatorForwarder) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	return LeaseReadForKeyThrough(f.Inner, ctx, key)
}

func (f CoordinatorForwarder) LeaseReadAllGroups(ctx context.Context) error {
	return LeaseReadAllGroupsThrough(f.Inner, ctx)
}

func (f CoordinatorForwarder) LeaseReadAllGroupsTimestamp(ctx context.Context) (uint64, error) {
	return LeaseReadAllGroupsTimestampThrough(f.Inner, ctx)
}

// IsLeaderForGroup / RaftLeaderForGroup / LeaseReadForGroup carry the
// group-keyed read-fence path. Without them every fence target falls back to
// key resolution, which re-introduces the legacy wide-column group collapse
// kv.ReadFenceTarget exists to prevent.
//
// A single-group coordinator has no group-keyed leadership; its one group is
// the group being asked about, so its own leadership answers.
func (f CoordinatorForwarder) IsLeaderForGroup(groupID uint64) bool {
	if router, ok := f.Inner.(GroupLeaderRoutableCoordinator); ok {
		return router.IsLeaderForGroup(groupID)
	}
	return f.Inner.IsLeader()
}

func (f CoordinatorForwarder) RaftLeaderForGroup(groupID uint64) string {
	if router, ok := f.Inner.(GroupLeaderRoutableCoordinator); ok {
		return router.RaftLeaderForGroup(groupID)
	}
	return f.Inner.RaftLeader()
}

func (f CoordinatorForwarder) LeaseReadForGroup(ctx context.Context, groupID uint64) (uint64, error) {
	if lr, ok := f.Inner.(interface {
		LeaseReadForGroup(context.Context, uint64) (uint64, error)
	}); ok {
		idx, err := lr.LeaseReadForGroup(ctx, groupID)
		return idx, errors.WithStack(err)
	}
	// A single-group coordinator has no group-keyed lease read; its one group
	// is the one being fenced, so the plain lease read is equivalent.
	return LeaseReadThrough(f.Inner, ctx)
}

// EnsureMutationsWriteAllowed and ObserveForwardedRequests reach
// Internal.Forward through optional-interface probes on the coordinator main
// hands the adapters. Without them follower-forwarded commits skip the
// MinWriteTSExclusive route floor and forwarded-write sampling goes dark.
func (f CoordinatorForwarder) EnsureMutationsWriteAllowed(muts []*pb.Mutation, commitTS uint64) error {
	gate, ok := f.Inner.(MutationWriteGate)
	if !ok {
		// A coordinator without a route table has no write floors to enforce.
		return nil
	}
	return errors.WithStack(gate.EnsureMutationsWriteAllowed(muts, commitTS))
}

func (f CoordinatorForwarder) ObserveForwardedRequests(reqs []*pb.Request) {
	if observer, ok := f.Inner.(interface{ ObserveForwardedRequests([]*pb.Request) }); ok {
		observer.ObserveForwardedRequests(reqs)
	}
}

func (f CoordinatorForwarder) EngineGroupIDForKey(key []byte) uint64 {
	if router, ok := f.Inner.(GroupRoutableCoordinator); ok {
		return router.EngineGroupIDForKey(key)
	}
	return 0
}

// ResolveOperationGroupForKey keeps receipt placement on the same route
// resolver as effect dispatch, including through startup and label wrappers.
func (f CoordinatorForwarder) ResolveOperationGroupForKey(key []byte) (uint64, error) {
	resolver, ok := f.Inner.(interface{ ResolveOperationGroupForKey([]byte) (uint64, error) })
	if !ok {
		return 0, errors.WithStack(ErrOperationGroupResolverUnavailable)
	}
	group, err := resolver.ResolveOperationGroupForKey(key)
	return group, errors.WithStack(err)
}

// JournalRouteVersion carries the route pin through coordinator wrappers.
// An unavailable pin must remain unavailable to the operation journal.
func (f CoordinatorForwarder) JournalRouteVersion() (uint64, bool) {
	source, ok := f.Inner.(interface{ JournalRouteVersion() (uint64, bool) })
	if !ok {
		return 0, false
	}
	return source.JournalRouteVersion()
}

// LocalLeaderGroupIDs lists the data groups this process leads. The Redis
// delta compactor's TTL-inline migration plans one scan per group from it.
// An Inner without the capability stays unknown rather than leading no group,
// so the migration falls back to whole-prefix scans instead of planning none.
func (f CoordinatorForwarder) LocalLeaderGroupIDs() ([]uint64, bool) {
	return LocalLeaderGroupIDsThrough(f.Inner)
}

func (f CoordinatorForwarder) RaftMembers(ctx context.Context) ([]RaftMember, error) {
	provider, ok := f.Inner.(RaftMembershipCoordinator)
	if !ok {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}
	members, err := provider.RaftMembers(ctx)
	return members, errors.WithStack(err)
}

func (f CoordinatorForwarder) RaftMembersForKey(ctx context.Context, key []byte) ([]RaftMember, error) {
	provider, ok := f.Inner.(RaftMembershipCoordinator)
	if !ok {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}
	members, err := provider.RaftMembersForKey(ctx, key)
	return members, errors.WithStack(err)
}
