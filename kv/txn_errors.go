package kv

import "github.com/cockroachdb/errors"

var (
	ErrTxnMetaMissing        = errors.New("txn meta missing")
	ErrTxnInvalidMeta        = errors.New("txn meta invalid")
	ErrTxnLocked             = errors.New("txn locked")
	ErrTxnCommitTSRequired   = errors.New("txn commit ts required")
	ErrTxnAlreadyCommitted   = errors.New("txn already committed")
	ErrTxnPrimaryKeyRequired = errors.New("txn primary key required")
)
