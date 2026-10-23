package store

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func stageSnapshotServingMetadata(batch *pebble.Batch, meta streamingMVCCRestoreMetadata) error {
	if err := stageSnapshotOwnership(batch, meta.migrationOwnership); err != nil {
		return err
	}
	if err := stageSnapshotPartitions(batch, meta.migrationPartitions); err != nil {
		return err
	}
	// Readiness has one raw row per job; publish it in the same batch.
	for _, state := range meta.migrationReadiness {
		if err := batch.Set(migrationReadyKey(state.JobID), encodeTargetStagedReadinessState(state), nil); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func readSnapshotPartitions(r io.Reader, version uint32) (MigrationOwnershipPartitionState, error) {
	if version < mvccSnapshotVersionV7 {
		return MigrationOwnershipPartitionState{}, nil
	}
	data, err := readMVCCSnapshotBytes(r, "snapshot ownership partitions")
	if err != nil {
		return MigrationOwnershipPartitionState{}, err
	}
	return decodeMigrationOwnershipPartitions(data)
}

func (s *pebbleStore) loadMigrationServingMetadata() ([]TargetStagedReadinessState, error) {
	readiness, err := s.loadPebbleTargetReadinessStatesFromDB()
	if err != nil {
		return nil, err
	}
	if _, err := s.readMigrationRetiredJobs(); err != nil {
		return nil, err
	}
	ownership, err := s.readMigrationOwnership()
	if err != nil {
		return nil, err
	}
	partitions, err := s.readMigrationOwnershipPartitions()
	if err != nil {
		return nil, err
	}
	s.migrationAdmission = buildMigrationServingAdmission(ownership, partitions)
	return readiness, nil
}

func stageSnapshotPartitions(batch *pebble.Batch, state MigrationOwnershipPartitionState) error {
	encoded, err := encodeMigrationOwnershipPartitions(state)
	if err != nil {
		return err
	}
	return errors.WithStack(batch.Set(migrationOwnershipPartitionMetaKey, encoded, nil))
}

func stageSnapshotOwnership(batch *pebble.Batch, states []MigrationOwnershipState) error {
	encoded, err := encodeMigrationOwnership(states)
	if err != nil {
		return err
	}
	return errors.WithStack(batch.Set(migrationOwnershipMetaKeyBytes, encoded, nil))
}
