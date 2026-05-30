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
