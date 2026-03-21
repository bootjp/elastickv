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
	ErrTxnPrimaryKeyRequired = errors.New("txn primary key required")
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
		return fmt.Sprintf("key: %s (%s): %s", string(e.key), e.detail, ErrTxnLocked)
	}
	return fmt.Sprintf("key: %s: %s", string(e.key), ErrTxnLocked)
}

func (e *TxnLockedError) Unwrap() error {
	return ErrTxnLocked
}
