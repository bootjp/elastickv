package kv

import (
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// txnIdentity is the key the FSM uses to recognise one cross-shard
// transaction: its locks, intents, commit record and rollback marker are all
// addressed by (primaryKey, startTS). StartTS is the caller's read snapshot,
// so two independent transactions on the same primary key can pick the same
// value whenever they read before either commits (for example two concurrent
// FUSE lease refreshes of one open handle).
type txnIdentity struct {
	primaryKey string
	startTS    uint64
}

// acquireTxnIdentity reserves the FSM identity of a cross-shard transaction
// for the duration of its prepare/commit phases.
//
// The FSM treats a PREPARE whose lock already carries the same identity as an
// idempotent replay. A second, distinct transaction with that identity would
// therefore overwrite the first one's intent rather than conflict with it, and
// whichever COMMIT lands second finds a commit record with a different commit
// timestamp and fails with ErrTxnInvalidMeta, which surfaces to the caller as
// ErrTransactionOutcomeUnknown. Rejecting the collision here, before anything
// is proposed, turns it into the definitive write conflict it really is: both
// transactions wrote the primary key from the same snapshot, so at most one of
// them may commit, and the loser can retry at a fresh snapshot.
//
// The reservation is process-local. It covers every transaction dispatched
// through this coordinator, which is where same-snapshot writers of one key
// meet in practice; writers on different nodes still rely on the FSM checks.
func (c *ShardedCoordinator) acquireTxnIdentity(primaryKey []byte, startTS uint64) (func(), error) {
	id := txnIdentity{primaryKey: string(primaryKey), startTS: startTS}
	c.inflightTxnMu.Lock()
	defer c.inflightTxnMu.Unlock()
	if _, busy := c.inflightTxns[id]; busy {
		return nil, errors.WithStack(store.NewWriteConflictError(primaryKey))
	}
	if c.inflightTxns == nil {
		c.inflightTxns = make(map[txnIdentity]struct{})
	}
	c.inflightTxns[id] = struct{}{}
	return func() {
		c.inflightTxnMu.Lock()
		delete(c.inflightTxns, id)
		c.inflightTxnMu.Unlock()
	}, nil
}
