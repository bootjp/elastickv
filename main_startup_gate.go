package main

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startupGatedCoordinator is the coordinator main hands the adapters. While
// the startup gate is closed (encryption rotate-on-startup) it refuses
// dispatches and timestamp allocation; everything else, including every
// optional capability the adapters type-assert, is forwarded by the embedded
// kv.CoordinatorForwarder.
type startupGatedCoordinator struct {
	kv.CoordinatorForwarder

	gate *startupPublicKVGate
}

func newStartupGatedCoordinator(inner kv.Coordinator, gate *startupPublicKVGate) startupGatedCoordinator {
	return startupGatedCoordinator{CoordinatorForwarder: kv.CoordinatorForwarder{Inner: inner}, gate: gate}
}

var _ kv.Coordinator = (*startupGatedCoordinator)(nil)
var _ kv.LeaseReadableCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.AllGroupsLeaseReadableCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.AllGroupsLeaseTimestampCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.GroupRoutableCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.LocalLeaderGroupsCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.RaftMembershipCoordinator = (*startupGatedCoordinator)(nil)
var _ kv.TimestampAllocatorProvider = (*startupGatedCoordinator)(nil)
var _ kv.ConfiguredTimestampAllocatorProvider = (*startupGatedCoordinator)(nil)
var _ kv.TimestampAllocator = (*startupGatedCoordinator)(nil)
var _ kv.TimestampAfterAllocator = (*startupGatedCoordinator)(nil)
var _ kv.AppliedReadTimestampVoucher = (*startupGatedCoordinator)(nil)
var _ kv.AppliedReadTimestampVoucherRevoker = (*startupGatedCoordinator)(nil)
var _ kv.AppliedReadTimestampVoucherSupport = (*startupGatedCoordinator)(nil)
var _ kv.MutationWriteGate = (*startupGatedCoordinator)(nil)

func (c startupGatedCoordinator) closed() bool {
	return c.gate != nil && c.gate.blocked()
}

// errStartupGateClosed carries codes.Unavailable. The stack wrapper keeps it
// reachable: grpc's status.FromError and the HTTP adapters' error mappers
// unwrap to the status, so callers still see Unavailable / 503.
func errStartupGateClosed() error {
	return errors.WithStack(status.Error(codes.Unavailable, "startup rotation has not completed"))
}

func (c startupGatedCoordinator) Dispatch(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if c.closed() {
		return nil, errStartupGateClosed()
	}
	resp, err := c.CoordinatorForwarder.Dispatch(ctx, reqs)
	return resp, errors.WithStack(err)
}

// TimestampAllocator returns the gate itself, so timestamps allocated through
// kv.TimestampAllocatorThrough are refused while the gate is closed too.
func (c startupGatedCoordinator) TimestampAllocator() kv.TimestampAllocator {
	return c
}

func (c startupGatedCoordinator) Next(ctx context.Context) (uint64, error) {
	if c.closed() {
		return 0, errStartupGateClosed()
	}
	ts, err := c.CoordinatorForwarder.Next(ctx)
	return ts, errors.WithStack(err)
}

func (c startupGatedCoordinator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	if c.closed() {
		return 0, errStartupGateClosed()
	}
	ts, err := c.CoordinatorForwarder.NextAfter(ctx, min)
	return ts, errors.WithStack(err)
}

type startupPublicKVGate struct {
	domain       *bootstrapDomain
	ready        atomic.Bool
	blockMutator func() bool
	blockRuntime func() bool
}

func (g *startupPublicKVGate) markReady() {
	if g != nil {
		g.ready.Store(true)
	}
}

func (g *startupPublicKVGate) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if g != nil && g.domain != nil && info != nil {
		return g.protectedUnary(ctx, req, info, handler)
	}
	if g != nil && info != nil && startupRotationGatedMethod(info.FullMethod) && g.blocked() {
		// Return a raw gRPC status so clients and retry policy see Unavailable.
		//nolint:wrapcheck
		return nil, status.Error(codes.Unavailable, "startup rotation has not completed")
	}
	return handler(ctx, req)
}

func (g *startupPublicKVGate) streamInterceptor(
	srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if g != nil && g.domain != nil && info != nil {
		return g.protectedStream(srv, stream, info, handler)
	}
	if g != nil && info != nil && startupRotationGatedMethod(info.FullMethod) && g.blocked() {
		// Return a raw gRPC status so clients and retry policy see Unavailable.
		//nolint:wrapcheck
		return status.Error(codes.Unavailable, "startup rotation has not completed")
	}
	return handler(srv, stream)
}

func (g *startupPublicKVGate) blocked() bool {
	if g == nil {
		return false
	}
	if !g.ready.Load() {
		return true
	}
	if g.blockRuntime != nil && g.blockRuntime() {
		return true
	}
	return g.blockMutator != nil && g.blockMutator()
}

func startupRotationGatedMethod(fullMethod string) bool {
	switch fullMethod {
	case pb.Internal_Forward_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName,
		pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName,
		pb.Internal_CleanupMigration_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName,
		pb.AdminForward_Forward_FullMethodName,
		pb.Distribution_GetTimestamp_FullMethodName,
		pb.Distribution_SplitRange_FullMethodName,
		pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_GetSplitMigrationCapability_FullMethodName,
		pb.Distribution_AttestMigrationOwnership_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
		pb.RaftAdmin_AddVoter_FullMethodName,
		pb.RaftAdmin_AddLearner_FullMethodName,
		pb.RaftAdmin_PromoteLearner_FullMethodName,
		pb.RaftAdmin_RemoveServer_FullMethodName,
		pb.RaftAdmin_TransferLeadership_FullMethodName,
		pb.EncryptionAdmin_BootstrapEncryption_FullMethodName,
		pb.EncryptionAdmin_RotateDEK_FullMethodName,
		pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		pb.EncryptionAdmin_ResyncSidecar_FullMethodName,
		pb.EncryptionAdmin_EnableStorageEnvelope_FullMethodName,
		pb.EncryptionAdmin_EnableRaftEnvelope_FullMethodName,
		pb.S3BlobFetch_PushChunkBlob_FullMethodName:
		return true
	default:
		return strings.HasPrefix(fullMethod, "/RawKV/") ||
			strings.HasPrefix(fullMethod, "/TransactionalKV/")
	}
}
