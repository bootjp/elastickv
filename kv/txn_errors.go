package kv

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/errors"
)

var (
	ErrTxnMetaMissing        = errors.New("txn meta missing")
	ErrTxnInvalidMeta        = errors.New("txn meta invalid")
	ErrTxnLocked             = errors.New("txn locked")
	ErrTxnCommitTSRequired   = errors.New("txn commit ts required")
	ErrTxnAlreadyCommitted   = errors.New("txn already committed")
	ErrTxnAlreadyAborted     = errors.New("txn already aborted")
	ErrTxnPrimaryKeyRequired = errors.New("txn primary key required")
	// ErrTxnDedupRequiresSingleShard is returned when a transaction request
	// carries OperationGroup.PrevCommitTS (the option-2 one-phase dedup probe
	// key) but its mutations or read keys span shards. The 2PC log builders
	// encode only CommitTS, so silently honoring such a request would drop
	// the probe at the FSM and let the original duplicate-elements anomaly
	// reappear. See codex P2 in PR #796 and the design doc.
	ErrTxnDedupRequiresSingleShard = errors.New("txn dedup (prev_commit_ts) requires a single-shard write set")
	// ErrTxnSecondaryRouteShiftedAfterPrimaryCommit is returned by
	// dispatchMultiShardTxn when a per-secondary commit (after primary
	// has durably committed) fails its M3 verifyComposed1 check —
	// i.e. the route catalog moved between primary-COMMIT and the
	// secondary-COMMIT, the secondary's FSM rejects with a Composed-1
	// sentinel, and we cannot transparently recover (the prepared
	// lock lives at the old gid; the new owner per the catalog has no
	// commit record).  The 2PC contract is half-broken at this point:
	// the primary's write is durable but at least one secondary's
	// write is missing.  Surfacing this explicitly (rather than
	// swallowing per the original best-effort semantic OR silently
	// landing the write on a stale owner per a dropped-gate fix) is
	// the least bad outcome — the caller knows the txn state is
	// uncertain and can do application-level recovery.
	//
	// This is a DIFFERENT sentinel from ErrComposed1Violation by
	// design: the M4 retry path in dispatchTxnWithComposed1Retry
	// matches ErrComposed1Violation / ErrComposed1VersionGCd and
	// would otherwise loop here, re-prewriting against the same old
	// gid that already has the first attempt's prepared lock — pure
	// wasted work since the route catalog won't move backward.
	// codex P1 on 6202b964 (PR #900) raised the silent-partial-commit
	// hazard; codex P1 on d8487672 (PR #900) raised the symmetric
	// hazard of disabling the gate.  This sentinel resolves both by
	// keeping the gate active and surfacing the error fatally.
	ErrTxnSecondaryRouteShiftedAfterPrimaryCommit = errors.New("txn secondary commit failed after primary commit: route catalog shifted")
)

type TxnLockedError struct {
	key    []byte
	detail string
}

func NewTxnLockedError(key []byte) error {
	return &TxnLockedError{key: bytes.Clone(key)}
}

func NewTxnLockedErrorWithDetail(key []byte, detail string) error {
	return &TxnLockedError{key: bytes.Clone(key), detail: detail}
}

func TxnLockedDetails(err error) ([]byte, string, bool) {
	var lockedErr *TxnLockedError
	if !errors.As(err, &lockedErr) {
		return nil, "", false
	}
	return bytes.Clone(lockedErr.key), lockedErr.detail, true
}

func (e *TxnLockedError) Error() string {
	if e.detail != "" {
		return fmt.Sprintf("key: %s (%s): %v", string(e.key), e.detail, ErrTxnLocked)
	}
	return fmt.Sprintf("key: %s: %v", string(e.key), ErrTxnLocked)
}

func (e *TxnLockedError) Unwrap() error {
	return ErrTxnLocked
}
