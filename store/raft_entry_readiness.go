package store

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Raw FSM batches update migration write trackers before their data writes.
// A zero child index deliberately defers the index to the enclosing entry.
func (s *mvccStore) stageMemoryReadiness(ctx context.Context, sc *memoryRaftEntry, state TargetStagedReadinessState, index uint64) error {
	if err := sc.allowData(); err != nil {
		return err
	}
	if index != 0 {
		if err := s.memoryEntryIndex(ctx, index); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return sc.fail(errors.WithStack(err))
	}
	if err := validateTargetStagedReadinessState(state); err != nil {
		return sc.fail(err)
	}
	dst := sc.staged
	dst.mtx.Lock()
	defer dst.mtx.Unlock()
	if _, retired := dst.migrationRetired[state.JobID]; retired {
		return sc.fail(errors.WithStack(ErrMigrationRetired))
	}
	if dst.readinessOverlay == nil {
		dst.readinessOverlay = make(map[uint64]TargetStagedReadinessState)
		dst.floorOverlay = make(map[uint64]uint64)
	}
	dst.readinessOverlay[state.JobID] = cloneTargetStagedReadinessState(state)
	if state.Armed {
		dst.lastCommitTS = max(dst.lastCommitTS, state.MinWriteTSExclusive)
		dst.floorOverlay[state.JobID] = max(dst.migrationHLCFloors[state.JobID], dst.floorOverlay[state.JobID], state.MinWriteTSExclusive)
	}
	sc.touched = true
	return nil
}

func (s *pebbleStore) stageEntryReadiness(ctx context.Context, sc *raftEntryScope, state TargetStagedReadinessState, index uint64) error {
	if err := validateEntryReadiness(ctx, sc, state, index); err != nil {
		return err
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db != sc.db {
		return sc.fail(errors.WithStack(ErrRaftEntryStoreSwapped))
	}
	if err := s.checkMigrationNotRetiredLocked(state.JobID); err != nil {
		return sc.fail(err)
	}
	if err := stageTargetReadinessGuard(sc.batch, state, 0, false); err != nil {
		return sc.fail(err)
	}
	if err := stageEntryReadinessFloor(sc, state); err != nil {
		return sc.fail(err)
	}
	if sc.readiness == nil {
		sc.readiness = make(map[uint64]TargetStagedReadinessState)
	}
	sc.readiness[state.JobID] = cloneTargetStagedReadinessState(state)
	sc.touched = true
	return nil
}

func validateEntryReadiness(ctx context.Context, sc *raftEntryScope, state TargetStagedReadinessState, index uint64) error {
	if err := sc.checkDataIndex(sc.appliedIndex); err != nil {
		return err
	}
	if index != 0 {
		if err := sc.checkIndex(index); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return sc.fail(errors.WithStack(err))
	}
	if err := validateTargetStagedReadinessState(state); err != nil {
		return sc.fail(err)
	}
	return nil
}

func stageEntryReadinessFloor(sc *raftEntryScope, state TargetStagedReadinessState) error {
	if state.Armed {
		floors, err := readEntryMigrationFloors(sc.batch)
		if err != nil {
			return err
		}
		floors[state.JobID] = max(floors[state.JobID], state.MinWriteTSExclusive)
		if err := sc.batch.Set(migrationHLCFloorMetaKeyBytes, encodeMigrationHLCFloors(floors), nil); err != nil {
			return errors.WithStack(err)
		}
		sc.advance = true
		sc.maxCommitTS = max(sc.maxCommitTS, state.MinWriteTSExclusive)
	}
	return nil
}

func mergedEntryReadiness(base []TargetStagedReadinessState, changes map[uint64]TargetStagedReadinessState) []TargetStagedReadinessState {
	view := cloneTargetStagedReadinessStates(base)
	for _, state := range changes {
		view = upsertTargetStagedReadinessStateCache(view, state)
	}
	return view
}
