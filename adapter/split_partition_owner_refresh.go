package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Refresh only closures needed by current serving owners. Protected SOURCE and
// unrelated negative evidence remain DENY, not an obligation to obtain ACKs.
func (s *DistributionServer) reconcileSplitServingOwners(ctx context.Context, id uint64) error {
	if !s.MigrationPartitionRuntimeConfigured(id) {
		return errors.WithStack(store.ErrNotSupported)
	}
	attempt, cancel := context.WithTimeout(ctx, ownershipRefreshBudget)
	defer cancel()
	r := s.ownershipRefresh
	if !r.mu.TryLock() {
		return errors.WithStack(errSplitMigrationVoterBarrierIncomplete)
	}
	defer r.mu.Unlock()
	g := r.groups[id]
	for attempt.Err() == nil {
		if err := s.fenceSplitPartitionGroup(attempt, id, g); err != nil {
			return err
		}
		view, before, index, err := readMigrationPartitionGroup(attempt, g)
		if err != nil {
			return err
		}
		view.Partitions = before
		if err := s.refreshSplitServingOwnerClosures(attempt, id, g, view, index); err != nil {
			return err
		}
		_, after, _, err := readMigrationPartitionGroup(attempt, g)
		if err != nil {
			return errors.WithStack(err)
		}
		same, err := sameMigrationPartitionFrontier(before, after)
		if err != nil || same {
			return err
		}
	}
	return errors.WithStack(attempt.Err())
}

func (s *DistributionServer) refreshSplitServingOwnerClosures(ctx context.Context, id uint64, g *kv.ShardGroup, view store.MigrationServingState, index uint64) error {
	candidates := migrationPartitionCandidates(view, view.Partitions)
	visited := make(map[int]bool)
	for at, candidate := range candidates {
		if visited[at] || !splitPartitionProofServing(view, candidate.geometry()) {
			continue
		}
		partitions, legacy, indices := migrationPartitionPriorClosure(candidates, at)
		for _, i := range indices {
			visited[i] = true
		}
		// Do not filter the selected closure: unknown legacy barriers and
		// negative overlaps must participate in the exact durable prior CAS.
		if err := s.reconcileMigrationPartitionPrior(ctx, id, g, index, view.Partitions, partitions, legacy); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(ctx.Err())
}

func splitPartitionHasServingOwner(view store.MigrationServingState) bool {
	for _, candidate := range migrationPartitionCandidates(view, view.Partitions) {
		if splitPartitionProofServing(view, candidate.geometry()) {
			return true
		}
	}
	return false
}

func splitPartitionProofServing(view store.MigrationServingState, p store.MigrationOwnershipPartition) bool {
	if p.Role != store.MigrationOwner {
		return false
	}
	if p.SourceTrackerVersion == 0 || !splitPartitionSourceEvidenceKnown(view, p) {
		return true
	}
	for _, guard := range view.Readiness {
		if splitPartitionSourceFenceMatches(p, guard) {
			return false
		}
	}
	return true
}

func splitPartitionSourceEvidenceKnown(view store.MigrationServingState, p store.MigrationOwnershipPartition) bool {
	if len(p.PartitionID) != 0 {
		return store.MigrationOwnershipPartitionEvidenceValid(view.Partitions, p)
	}
	for _, old := range view.Ownership {
		if old.JobID == p.MigrationJobID && store.MigrationOwnershipEvidenceValid(old) && old.SourceTrackerVersion == p.SourceTrackerVersion {
			return old.SourceAppliedIndex == p.SourceAppliedIndex && bytes.Equal(old.DescriptorHash, p.DescriptorHash)
		}
	}
	return false
}

func splitPartitionSourceFenceMatches(p store.MigrationOwnershipPartition, guard store.TargetStagedReadinessState) bool {
	if !splitPartitionSourceFenceValid(guard) || guard.JobID != p.MigrationJobID || guard.ExpectedCutoverVersion < p.SourceTrackerVersion {
		return false
	}
	return guard.MinWriteTSExclusive >= p.MinWriteTSExclusive && bytes.Compare(guard.RouteStart, p.RouteStart) <= 0 &&
		(len(guard.RouteEnd) == 0 || (len(p.RouteEnd) != 0 && bytes.Compare(guard.RouteEnd, p.RouteEnd) >= 0))
}

func splitPartitionSourceFenceValid(guard store.TargetStagedReadinessState) bool {
	if !guard.Armed || guard.JobID == 0 || guard.MigrationJobID != guard.JobID || guard.MinWriteTSExclusive == 0 || guard.RetentionPinTS == 0 {
		return false
	}
	if guard.SourceReadFence && !guard.SourceWriteFence {
		return false
	}
	return guard.SourceWriteFence && splitPartitionSourceGuardBoundsValid(guard) && splitPartitionSourceTransferGuardValid(guard)
}

func splitPartitionSourceGuardBoundsValid(guard store.TargetStagedReadinessState) bool {
	return guard.RouteEnd == nil || bytes.Compare(guard.RouteStart, guard.RouteEnd) < 0
}

func splitPartitionSourceTransferGuardValid(guard store.TargetStagedReadinessState) bool {
	return guard.TransferSizeGuardVersion == 0 || (guard.TransferSizeGuardVersion == store.MigrationTransferSizeGuardVersion && guard.TrackWrites)
}
