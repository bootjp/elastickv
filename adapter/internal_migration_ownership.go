package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const migrationOwnershipProtocol = 1

// WithInternalMigrationOwnershipReady must be wired to the real public
// proposal/read and deterministic-apply integrations, not an environment flag.
// Until then this binary must not advertise complete ownership readiness.
func WithInternalMigrationOwnershipReady(ready func() bool) InternalOption {
	return func(i *Internal) { i.migrationOwnershipReady = ready }
}

func (i *Internal) ownershipAdmissionReady() bool {
	return i.migrationOwnershipReady != nil && i.migrationOwnershipReady()
}

func ownershipCleanupMode(mode pb.MigrationCleanupMode) bool {
	return mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_ACQUIRE ||
		mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR ||
		mode == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_REFRESH
}

func (i *Internal) probeMigrationOwnership(ctx context.Context, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	resp := &pb.ProbeMigrationStateResponse{}
	if !i.ownershipAdmissionReady() {
		return resp, nil
	}
	engine, ok := i.leader.(raftengine.AppliedConfigurationReader)
	if !ok {
		return resp, nil
	}
	index, supported := engine.AppliedConfigurationIndex()
	if !supported || index == 0 {
		return resp, nil
	}
	reader, ok := i.store.(store.MigrationOwnershipStore)
	if !ok {
		return resp, nil
	}
	if _, ok := i.store.(store.MigrationOwnershipRefresher); !ok {
		return resp, nil
	}
	view, err := reader.MigrationServingState(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	generation, valid := migrationOwnershipGeneration(view.Ownership)
	if !valid {
		return resp, nil
	}
	resp.OwnershipGeneration, resp.OwnershipProtocol, resp.AppliedConfigurationIndex = generation, migrationOwnershipProtocol, index
	if req.GetKind() == pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY {
		resp.Ready = true
		return resp, nil
	}
	return i.probeMigrationOwnershipApplied(ctx, req, resp, view)
}

func migrationOwnershipGeneration(states []store.MigrationOwnershipState) (string, bool) {
	var generation string
	for _, state := range states {
		if len(state.Generation) < 16 || len(state.DescriptorHash) != 32 || len(state.Receipts) == 0 {
			return "", false
		}
		if generation != "" && state.Generation != generation {
			return "", false
		}
		generation = state.Generation
	}
	return generation, true
}

func (i *Internal) probeMigrationOwnershipApplied(ctx context.Context, req *pb.ProbeMigrationStateRequest, resp *pb.ProbeMigrationStateResponse, view store.MigrationServingState) (*pb.ProbeMigrationStateResponse, error) {
	for _, state := range view.Ownership {
		if state.JobID != req.GetJobId() {
			continue
		}
		resp.Ownership = kv.MigrationOwnershipProofFromState(state)
		resp.Ready = req.GetOwnership() == nil || proto.Equal(req.GetOwnership(), resp.GetOwnership())
		retirement, ok := i.store.(store.MigrationRetirementReader)
		if !ok {
			resp.Ready = false
			return resp, nil
		}
		retired, err := retirement.MigrationRetired(ctx, state.JobID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		resp.OwnershipCleared = retired
		return resp, nil
	}
	return resp, nil
}
