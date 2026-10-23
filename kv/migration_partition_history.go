package kv

import (
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// MigrationPartitionHistoryPresent detects durable admission obligations even
// when migration activation is disabled. Receipt-only history still requires
// compatible members. This is not a serving or ownership authorization.
func MigrationPartitionHistoryPresent(ctx context.Context, st store.MVCCStore) (bool, error) {
	if st == nil {
		return false, errors.WithStack(ErrInvalidRequest)
	}
	if reader, supported := st.(store.MigrationOwnershipPartitionStore); supported {
		state, err := reader.MigrationOwnershipPartitions(ctx)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if len(state.Partitions) != 0 || len(state.Receipts) != 0 {
			return true, nil
		}
	}
	view, err := readMigrationServingState(ctx, st)
	if err != nil {
		return false, err
	}
	if len(view.Ownership) != 0 {
		return true, nil
	}
	for _, guard := range view.Readiness {
		if guard.Armed {
			return true, nil
		}
	}
	return false, nil
}

// MigrationPartitionRequestAdmissionReady reports installed proposal/store
// integration, not an active epoch or migration activation permission.
func MigrationPartitionRequestAdmissionReady(tx Transactional) bool {
	switch t := tx.(type) {
	case *TransactionManager:
		if !migrationTransactionManagerReady(t) {
			return false
		}
		_, supported := t.migrationAdmission.Load().store.(store.MigrationOwnershipPartitionStore)
		return supported
	case *LeaderProxy:
		return t != nil && MigrationPartitionRequestAdmissionReady(t.tm)
	case *leaseRefreshingTxn:
		return t != nil && MigrationPartitionRequestAdmissionReady(t.inner)
	case *timestampAdmissionTransactional:
		return t != nil && MigrationPartitionRequestAdmissionReady(t.inner)
	default:
		return false
	}
}

// MigrationPartitionReadAdmissionReady reports the public read/store pairing;
// the runtime additionally checks its installed committed-root consumer.
func (s *ShardStore) MigrationPartitionReadAdmissionReady(groupID uint64) bool {
	if !s.MigrationReadAdmissionReady(groupID) {
		return false
	}
	_, supported := s.groups[groupID].Store.(store.MigrationOwnershipPartitionStore)
	return supported
}
