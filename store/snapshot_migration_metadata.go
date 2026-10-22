package store

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

func (s *pebbleStore) loadMigrationServingMetadata() ([]TargetStagedReadinessState, error) {
	readiness, err := s.loadPebbleTargetReadinessStatesFromDB()
	if err != nil {
		return nil, err
	}
	if _, err := s.readMigrationRetiredJobs(); err != nil {
		return nil, err
	}
	if _, err := s.readMigrationOwnership(); err != nil {
		return nil, err
	}
	return readiness, nil
}

func stageSnapshotOwnership(batch *pebble.Batch, states []MigrationOwnershipState) error {
	encoded, err := encodeMigrationOwnership(states)
	if err != nil {
		return err
	}
	return errors.WithStack(batch.Set(migrationOwnershipMetaKeyBytes, encoded, nil))
}
