package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
)

const MigrationTransferSizeCapability = "cap_migration_transfer_size_v1"

// WithInternalMigrationTransferSizeGuardReady opts into capability advertising
// only after runtime wiring has verified public writers and member admission.
// Ownership protocol 2 alone is not evidence that size admission is supported.
func WithInternalMigrationTransferSizeGuardReady(ready func() bool) InternalOption {
	return func(i *Internal) { i.transferSizeReady = ready }
}

// MigrationTransferSizeGuardSupported can be true on a fresh learner before
// its first configuration is applied. It is never an active-guard ACK.
func (i *Internal) MigrationTransferSizeGuardSupported() bool {
	if i.transferSizeReady == nil || !i.transferSizeReady() ||
		i.migrationProposer == nil {
		return false
	}
	if _, ok := i.store.(store.MigrationTargetReadinessReader); !ok {
		return false
	}
	if _, ok := i.store.(store.MigrationTargetReadinessRaftWriter); !ok {
		return false
	}
	return true
}

// MigrationTransferSizeGuardReady additionally requires an applied membership
// epoch. Activation still compares that epoch at proposal and deterministic apply.
func (i *Internal) MigrationTransferSizeGuardReady() bool {
	if !i.MigrationTransferSizeGuardSupported() || !i.ownershipAdmissionReady() {
		return false
	}
	reader, ok := i.leader.(raftengine.AppliedConfigurationReader)
	if !ok {
		return false
	}
	index, supported := reader.AppliedConfigurationIndex()
	return supported && index != 0
}

func (i *Internal) validateTransferSizeGuardActivation(ctx context.Context, req *pb.TargetStagedReadinessRequest) error {
	if req.GetTransferSizeGuardVersion() == 0 {
		return nil
	}
	if !validTransferSizeGuardActivation(req) {
		return grpcStatusErrorf(codes.InvalidArgument, "transfer size guard requires supported version, source tracker and configuration")
	}
	if !i.MigrationTransferSizeGuardReady() {
		return grpcStatusErrorf(codes.FailedPrecondition, "migration transfer size admission is not wired")
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	reader, ok := i.leader.(raftengine.AppliedConfigurationReader)
	if !ok {
		return grpcStatusErrorf(codes.FailedPrecondition, "migration configuration reader is not wired")
	}
	index, supported := reader.AppliedConfigurationIndex()
	if !supported || index != req.GetExpectedConfigurationIndex() {
		return grpcStatusErrorf(codes.FailedPrecondition, "migration transfer size configuration changed")
	}
	return nil
}

func validTransferSizeGuardActivation(req *pb.TargetStagedReadinessRequest) bool {
	return req.GetTransferSizeGuardVersion() == store.MigrationTransferSizeGuardVersion && req.GetArmed() && req.GetTrackWrites() &&
		req.GetRequireConfigurationIndex() && req.GetExpectedConfigurationIndex() != 0
}
