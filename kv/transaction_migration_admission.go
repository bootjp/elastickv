package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
)

type transactionMigrationAdmission struct {
	store   store.MVCCStore
	groupID uint64
}

// WithMigrationRequestAdmission rechecks the final request after raw queue
// waiting, immediately before proposal. Apply independently repeats admission.
func WithMigrationRequestAdmission(st store.MVCCStore, groupID uint64) TransactionOption {
	return func(t *TransactionManager) {
		t.migrationAdmission.Store(&transactionMigrationAdmission{store: st, groupID: groupID})
	}
}

func MigrationRequestAdmissionReady(tx Transactional) bool {
	switch t := tx.(type) {
	case *TransactionManager:
		return migrationTransactionManagerReady(t)
	case *LeaderProxy:
		return t != nil && MigrationRequestAdmissionReady(t.tm)
	case *leaseRefreshingTxn:
		return t != nil && MigrationRequestAdmissionReady(t.inner)
	case *timestampAdmissionTransactional:
		return t != nil && MigrationRequestAdmissionReady(t.inner)
	default:
		return false
	}
}

func migrationTransactionManagerReady(t *TransactionManager) bool {
	if t == nil {
		return false
	}
	a := t.migrationAdmission.Load()
	return a != nil && a.store != nil && a.groupID != 0
}

func configureMigrationRequestAdmission(tx Transactional, st store.MVCCStore, groupID uint64) {
	switch t := tx.(type) {
	case *TransactionManager:
		WithMigrationRequestAdmission(st, groupID)(t)
	case *LeaderProxy:
		configureMigrationRequestAdmission(t.tm, st, groupID)
	case *leaseRefreshingTxn:
		configureMigrationRequestAdmission(t.inner, st, groupID)
	case *timestampAdmissionTransactional:
		configureMigrationRequestAdmission(t.inner, st, groupID)
	}
}

func (a *transactionMigrationAdmission) apply(ctx context.Context, proposer raftengine.Proposer, reqs []*pb.Request, observer ProposalObserver) (uint64, []error, error) {
	results := make([]error, len(reqs))
	accepted := make([]*pb.Request, 0, len(reqs))
	positions := make([]int, 0, len(reqs))
	for i, req := range reqs {
		results[i] = CheckMigrationRequest(ctx, a.store, a.groupID, req)
		if results[i] == nil {
			accepted, positions = append(accepted, req), append(positions, i)
		}
	}
	if len(accepted) == 0 {
		return 0, results, nil
	}
	index, applied, err := applyRequests(ctx, proposer, accepted, observer)
	if err != nil {
		return 0, nil, err
	}
	for i, position := range positions {
		results[position] = applied[i]
	}
	return index, results, nil
}
