package store

import (
	"context"

	"github.com/cockroachdb/errors"
)

// MigrationServingAdmissionReader is an optional compact, coherent serving
// view. It retains unknown legacy barriers, but excludes adopted legacy jobs
// and historical partition receipts not referenced by the active frontier.
// Full-history readers remain available for control CAS and receipt replay.
type MigrationServingAdmissionReader interface {
	MigrationServingAdmissionState(context.Context) (MigrationServingState, error)
}

// MigrationServingSummary is a catalog floor and conservative metadata-presence
// hint, never ownership authority. Present includes unarmed readiness and
// receipt-only history; an empty hint must not bypass route/configuration gates.
type MigrationServingSummary struct {
	CatalogVersionFloor uint64
	Present             bool
}

type MigrationServingSummaryReader interface {
	MigrationServingSummary(context.Context) (MigrationServingSummary, error)
}

// Protected by the store metadata mutex. Replaced, never mutated, after a
// successful metadata commit or validated restore. Readiness is kept in the
// existing cache under that same mutex rather than duplicated here.
type migrationServingAdmissionIndex struct {
	state   MigrationServingState
	summary MigrationServingSummary
}

func buildMigrationServingAdmission(ownership []MigrationOwnershipState, partitions MigrationOwnershipPartitionState) migrationServingAdmissionIndex {
	index := migrationServingAdmissionIndex{}
	index.summary.Present = len(ownership) != 0 || partitionHasEvidence(partitions)
	consumed := partitionAdoptedJobs(partitions)
	receipts := make(map[string]MigrationOwnershipPartitionReceipt, len(partitions.Receipts))
	for _, r := range partitions.Receipts {
		receipts[string(r.ReceiptID)] = r
	}
	index.state.Ownership, index.summary.CatalogVersionFloor = admissionLegacyOwnership(ownership, consumed)
	index.state.Partitions.Generation = partitions.Generation
	index.state.Partitions.Partitions = partitions.Partitions
	used := make(map[string]bool)
	for _, p := range partitions.Partitions {
		index.summary.CatalogVersionFloor = max(index.summary.CatalogVersionFloor, p.CatalogVersion)
		id := string(p.ReceiptID)
		if !used[id] {
			index.state.Partitions.Receipts = append(index.state.Partitions.Receipts, receipts[id])
			used[id] = true
		}
	}
	index.state.Ownership = cloneMigrationOwnership(index.state.Ownership)
	index.state.Partitions = clonePartitionState(index.state.Partitions)
	return index
}

func partitionAdoptedJobs(state MigrationOwnershipPartitionState) map[uint64]bool {
	consumed := make(map[uint64]bool)
	for _, receipt := range state.Receipts {
		for _, ref := range receipt.Transition.Before {
			if ref.LegacyJobID != 0 {
				consumed[ref.LegacyJobID] = true
			}
		}
		// Guarded Acquire/Clear creates a new job certificate and frontier in
		// the same commit; its Before references name the superseded partition,
		// not the newly created job. That new ledger is history, not a second
		// certificate competing with the frontier at the same catalog version.
		if receipt.Transition.Operation == MigrationOwnershipPartitionJobOperation {
			for _, after := range receipt.Transition.After {
				consumed[after.MigrationJobID] = true
			}
		}
	}
	return consumed
}

func admissionLegacyOwnership(ownership []MigrationOwnershipState, consumed map[uint64]bool) ([]MigrationOwnershipState, uint64) {
	var retained []MigrationOwnershipState
	var floor uint64
	for _, old := range ownership {
		if !MigrationOwnershipEvidenceValid(old) {
			retained = append(retained, old)
			continue
		}
		floor = max(floor, old.CatalogVersion)
		if !consumed[old.JobID] {
			retained = append(retained, old)
		}
	}
	return retained, floor
}

func (i migrationServingAdmissionIndex) view(readiness []TargetStagedReadinessState) MigrationServingState {
	return MigrationServingState{Ownership: cloneMigrationOwnership(i.state.Ownership),
		Partitions: clonePartitionState(i.state.Partitions), Readiness: cloneReadinessStates(readiness)}
}

func (i migrationServingAdmissionIndex) withReadiness(readiness []TargetStagedReadinessState) MigrationServingSummary {
	summary := i.summary
	summary.Present = summary.Present || len(readiness) != 0
	return summary
}

func (s *mvccStore) MigrationServingAdmissionState(ctx context.Context) (MigrationServingState, error) {
	if err := ctx.Err(); err != nil {
		return MigrationServingState{}, errors.WithStack(err)
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.migrationAdmission.view(s.migrationReadinessCache), nil
}

func (s *pebbleStore) MigrationServingAdmissionState(ctx context.Context) (MigrationServingState, error) {
	if err := ctx.Err(); err != nil {
		return MigrationServingState{}, errors.WithStack(err)
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.migrationAdmission.view(s.migrationReadinessCache), nil
}

func (s *mvccStore) MigrationServingSummary(ctx context.Context) (MigrationServingSummary, error) {
	if err := ctx.Err(); err != nil {
		return MigrationServingSummary{}, errors.WithStack(err)
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.migrationAdmission.withReadiness(s.migrationReadinessCache), nil
}

func (s *pebbleStore) MigrationServingSummary(ctx context.Context) (MigrationServingSummary, error) {
	if err := ctx.Err(); err != nil {
		return MigrationServingSummary{}, errors.WithStack(err)
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.migrationAdmission.withReadiness(s.migrationReadinessCache), nil
}

var _ MigrationServingAdmissionReader = (*mvccStore)(nil)
var _ MigrationServingAdmissionReader = (*pebbleStore)(nil)
var _ MigrationServingSummaryReader = (*mvccStore)(nil)
var _ MigrationServingSummaryReader = (*pebbleStore)(nil)
