package store

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func (s *pebbleStore) readMigrationOwnershipPartitions() (MigrationOwnershipPartitionState, error) {
	data, closer, err := s.db.Get(migrationOwnershipPartitionMetaKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return MigrationOwnershipPartitionState{}, nil
	}
	if err != nil {
		return MigrationOwnershipPartitionState{}, errors.WithStack(err)
	}
	defer func() { _ = closer.Close() }()
	return decodeMigrationOwnershipPartitions(data)
}

func (s *mvccStore) MigrationOwnershipPartitions(ctx context.Context) (MigrationOwnershipPartitionState, error) {
	view, err := s.MigrationServingState(ctx)
	return view.Partitions, err
}

func (s *pebbleStore) MigrationOwnershipPartitions(ctx context.Context) (MigrationOwnershipPartitionState, error) {
	view, err := s.MigrationServingState(ctx)
	return view.Partitions, err
}

func partitionConfiguration(value []byte, err error) (uint64, error) {
	if errors.Is(err, ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	index, err := DecodeMigrationConfigurationIndex(value)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return index, nil
}

func (s *mvccStore) partitionConfigurationLocked() (uint64, error) {
	v, ok := s.tree.Get(MigrationConfigurationIndexKey())
	if !ok {
		return 0, nil
	}
	versions, _ := v.([]VersionedValue)
	latest, ok := latestVisible(versions, ^uint64(0))
	if !ok || latest.Tombstone {
		return 0, nil
	}
	return partitionConfiguration(latest.Value, nil)
}

func (s *pebbleStore) partitionConfigurationLocked(ctx context.Context) (uint64, error) {
	return partitionConfiguration(s.getAt(ctx, MigrationConfigurationIndexKey(), ^uint64(0)))
}

func (s *mvccStore) syncPartitionJobLocked(updated []MigrationOwnershipState, requested MigrationOwnershipState, operation MigrationOwnershipOperation, index uint64) (MigrationOwnershipPartitionState, error) {
	var cfg uint64
	if partitionHasEvidence(s.migrationPartitions) {
		var err error
		cfg, err = s.partitionConfigurationLocked()
		if err != nil {
			return s.migrationPartitions, err
		}
	}
	grant := partitionJobOwnerGrant(requested, operation, s.migrationReadinessCache, s.migrationPromotions[requested.JobID].Done)
	return syncPartitionJob(s.migrationPartitions, s.migrationOwnership, updated, requested, operation, index, cfg, grant)
}

func (s *pebbleStore) syncPartitionJobLocked(ctx context.Context, partitions MigrationOwnershipPartitionState, old, updated []MigrationOwnershipState, requested MigrationOwnershipState, operation MigrationOwnershipOperation, index uint64) (MigrationOwnershipPartitionState, error) {
	var cfg uint64
	if partitionHasEvidence(partitions) {
		var err error
		cfg, err = s.partitionConfigurationLocked(ctx)
		if err != nil {
			return partitions, err
		}
	}
	s.mtx.RLock()
	guards := cloneReadinessStates(s.migrationReadinessCache)
	s.mtx.RUnlock()
	promotions, err := s.readPebblePromotionStates()
	if err != nil {
		return partitions, err
	}
	grant := partitionJobOwnerGrant(requested, operation, guards, promotions[requested.JobID].Done)
	return syncPartitionJob(partitions, old, updated, requested, operation, index, cfg, grant)
}

func partitionJobOwnerGrant(requested MigrationOwnershipState, operation MigrationOwnershipOperation, guards []TargetStagedReadinessState, promoted bool) bool {
	if operation == MigrationOwnershipAcquire {
		return true
	} // acquisition guard was already validated
	if operation != MigrationOwnershipClear {
		return false
	}
	for _, guard := range guards {
		if ownershipGuardMatchesRange(requested, guard) && ownershipClearMatches(requested, guard, promoted) {
			return true
		}
	}
	return false
}

func (s *mvccStore) ApplyMigrationOwnershipPartition(ctx context.Context, t MigrationOwnershipPartitionTransition, index uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	cfg, err := s.partitionConfigurationLocked()
	if err != nil {
		return err
	}
	updated, err := applyPartitionTransition(s.migrationPartitions, s.migrationOwnership, t, index, cfg)
	if err != nil {
		return err
	}
	s.migrationPartitions = updated
	s.migrationAdmission = buildMigrationServingAdmission(s.migrationOwnership, updated)
	return nil
}

func (s *pebbleStore) ApplyMigrationOwnershipPartition(ctx context.Context, t MigrationOwnershipPartitionTransition, index uint64) error {
	// Typed migration controls are separate Raft entries. A raw entry already
	// owns applyMu; neither reenter it nor commit metadata outside its batch.
	if s.raftEntryScopeFrom(ctx) != nil {
		return errors.WithStack(ErrMigrationOwnership)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	state, err := s.readMigrationOwnershipPartitions()
	if err != nil {
		return err
	}
	legacy, err := s.readMigrationOwnership()
	if err != nil {
		return err
	}
	cfg, err := s.partitionConfigurationLocked(ctx)
	if err != nil {
		return err
	}
	updated, err := applyPartitionTransition(state, legacy, t, index, cfg)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := stageSnapshotPartitions(batch, updated); err != nil {
		return err
	}
	if err := s.stagePartitionAppliedIndex(batch, index); err != nil {
		return err
	}
	if err := batch.Commit(s.cleanupWriteOpts(index)); err != nil {
		return errors.WithStack(err)
	}
	s.mtx.Lock()
	s.migrationAdmission = buildMigrationServingAdmission(legacy, updated)
	s.mtx.Unlock()
	return nil
}

// Late receipt replay must never rewind the durable Raft watermark.
func (s *pebbleStore) stagePartitionAppliedIndex(batch *pebble.Batch, index uint64) error {
	persisted, err := readPebbleUint64(s.db, metaAppliedIndexBytes)
	if err != nil {
		return err
	}
	if index < persisted {
		index = persisted
	}
	return setPebbleUint64InBatch(batch, metaAppliedIndexBytes, index)
}

var _ MigrationOwnershipPartitionStore = (*mvccStore)(nil)
var _ MigrationOwnershipPartitionStore = (*pebbleStore)(nil)
