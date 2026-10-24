package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func MigrationOwnershipStateFromProto(jobID uint64, proof *pb.MigrationOwnershipProof) (store.MigrationOwnershipState, error) {
	if proof == nil || len(proof.GetGeneration()) < 16 || len(proof.GetGeneration()) > 128 || proof.GetRouteId() == 0 || len(proof.GetDescriptorHash()) != 32 {
		return store.MigrationOwnershipState{}, errors.WithStack(ErrInvalidRequest)
	}
	var role store.MigrationOwnershipRole
	switch proof.GetRole() {
	case pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER:
		role = store.MigrationOwner
	case pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER:
		role = store.MigrationNonOwner
	case pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_UNSPECIFIED:
		return store.MigrationOwnershipState{}, errors.WithStack(ErrInvalidRequest)
	default:
		return store.MigrationOwnershipState{}, errors.WithStack(ErrInvalidRequest)
	}
	return store.MigrationOwnershipState{Generation: proof.GetGeneration(), JobID: jobID,
		WriteFloorKnown: proof.GetWriteFloorKnown(), SourceTrackerVersion: proof.GetSourceTrackerVersion(), SourceAppliedIndex: proof.GetSourceAppliedIndex(),
		LocalGroupID: proof.GetLocalGroupId(), OwnerGroupID: proof.GetOwnerGroupId(), CatalogVersion: proof.GetCatalogVersion(),
		MinWriteTSExclusive: proof.GetMinWriteTsExclusive(), RouteStart: bytes.Clone(proof.GetRouteStart()),
		RouteEnd: bytes.Clone(proof.GetRouteEnd()), RouteID: proof.GetRouteId(), ParentRouteID: proof.GetParentRouteId(),
		Staged: proof.GetStaged(), Role: role, DescriptorHash: bytes.Clone(proof.GetDescriptorHash())}, nil
}

func MigrationOwnershipProofFromState(state store.MigrationOwnershipState) *pb.MigrationOwnershipProof {
	role := pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_NONOWNER
	if state.Role == store.MigrationOwner {
		role = pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER
	}
	return &pb.MigrationOwnershipProof{Generation: state.Generation, LocalGroupId: state.LocalGroupID,
		WriteFloorKnown: state.WriteFloorKnown, SourceTrackerVersion: state.SourceTrackerVersion, SourceAppliedIndex: state.SourceAppliedIndex,
		OwnerGroupId: state.OwnerGroupID, CatalogVersion: state.CatalogVersion, MinWriteTsExclusive: state.MinWriteTSExclusive,
		RouteStart: bytes.Clone(state.RouteStart), RouteEnd: bytes.Clone(state.RouteEnd), RouteId: state.RouteID,
		ParentRouteId: state.ParentRouteID, Staged: state.Staged, Role: role, DescriptorHash: bytes.Clone(state.DescriptorHash)}
}

func migrationOwnershipControlMode(mode pb.MigrationCleanupMode) bool {
	return mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_ACQUIRE ||
		mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR ||
		mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE ||
		mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH
}

func (f *kvFSM) verifyLegacyMigrationMetadataClear(ctx context.Context, jobID uint64) error {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	guards, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, guard := range guards {
		if guard.JobID == jobID && guard.Armed {
			return errors.WithStack(store.ErrMigrationOwnership)
		}
	}
	return nil
}

func (f *kvFSM) applyMigrationOwnershipControl(ctx context.Context, req *pb.CleanupMigrationRequest) any {
	if !req.GetRequireConfigurationIndex() || req.GetExpectedConfigurationIndex() == 0 {
		return errors.WithStack(ErrMigrationConfigurationChanged)
	}
	state, err := MigrationOwnershipStateFromProto(req.GetJobId(), req.GetOwnership())
	if err != nil {
		return err
	}
	if f.shardGroupID == 0 || state.LocalGroupID != f.shardGroupID {
		return errors.WithStack(ErrInvalidRequest)
	}
	state.ConfigurationIndex = f.AppliedConfigurationIndex()
	if !migrationOwnershipSourceModeValid(req.GetMode(), state) {
		return errors.WithStack(ErrInvalidRequest)
	}
	err = f.applyMigrationOwnershipOperation(ctx, req.GetMode(), state)
	if err == nil {
		return store.CleanupVersionsResult{Done: true}
	}
	if migrationOwnershipOrdinaryError(err) {
		return err
	}
	return haltErr(errors.Wrap(errors.Mark(err, ErrMigrationCleanupApply), "kv/fsm: ownership control"))
}

func migrationOwnershipOrdinaryError(err error) bool {
	return errors.Is(err, store.ErrMigrationOwnership) || errors.Is(err, store.ErrMigrationRetired) || errors.Is(err, store.ErrNotSupported)
}

func (f *kvFSM) applyMigrationOwnershipOperation(ctx context.Context, mode pb.MigrationCleanupMode, state store.MigrationOwnershipState) error {
	writer, ok := f.store.(store.MigrationOwnershipStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	switch mode {
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_ACQUIRE,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE:
		return errors.WithStack(writer.AcquireMigrationOwnership(ctx, state, f.pendingApplyIdx))
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR:
		return errors.WithStack(writer.ClearMigrationStateWithOwnership(ctx, state, f.pendingApplyIdx))
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH:
		refresher, ok := f.store.(store.MigrationOwnershipRefresher)
		if !ok {
			return errors.WithStack(store.ErrNotSupported)
		}
		return errors.WithStack(refresher.RefreshMigrationOwnership(ctx, state, f.pendingApplyIdx))
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_UNSPECIFIED,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA:
		return errors.WithStack(ErrInvalidRequest)
	default:
		return errors.WithStack(ErrInvalidRequest)
	}
}

func migrationOwnershipSourceModeValid(mode pb.MigrationCleanupMode, state store.MigrationOwnershipState) bool {
	switch mode {
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE:
		return state.SourceTrackerVersion != 0 && state.SourceAppliedIndex == 0 && state.WriteFloorKnown
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH:
		return true
	case pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_UNSPECIFIED, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_PARTITION,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_ACQUIRE,
		pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR:
		return state.SourceTrackerVersion == 0 && state.SourceAppliedIndex == 0
	default:
		return false
	}
}
