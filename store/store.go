package store

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
)

var ErrKeyNotFound = errors.New("not found")
var ErrUnknownOp = errors.New("unknown op")
var ErrNotSupported = errors.New("not supported")
var ErrInvalidChecksum = errors.New("invalid checksum")
var ErrWriteConflict = errors.New("write conflict")
var ErrExpired = errors.New("expired")

type KVPair struct {
	Key   []byte
	Value []byte
}

var Tombstone = []byte{0x00}

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error
	Close() error
}

type ScanStore interface {
	Store
	Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error)
}

// HybridClock provides monotonically increasing timestamps (HLC).
type HybridClock interface {
	Now() uint64
}

// MVCCStore extends Store with multi-version concurrency control helpers.
// Reads can be evaluated at an arbitrary timestamp, and commits validate
// conflicts against the latest committed version.
type MVCCStore interface {
	ScanStore
	TTLStore

	// GetAt returns the newest version whose commit timestamp is <= ts.
	GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error)
	// LatestCommitTS returns the commit timestamp of the newest version.
	// The boolean reports whether the key has any version.
	LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error)
	// ApplyMutations atomically validates and appends the provided mutations.
	// It must return ErrWriteConflict if any key has a newer commit timestamp
	// than startTS.
	ApplyMutations(ctx context.Context, mutations []*KVPairMutation, startTS, commitTS uint64) error
}

// KVPairMutation is a small helper struct for MVCC mutation application.
type KVPairMutation struct {
	Op    OpType
	Key   []byte
	Value []byte
	// ExpireAt is an HLC timestamp; 0 means no TTL.
	ExpireAt uint64
}

type TTLStore interface {
	Store
	Expire(ctx context.Context, key []byte, ttl int64) error
	PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error
	TxnWithTTL(ctx context.Context, f func(ctx context.Context, txn TTLTxn) error) error
}

type ScanTTLStore interface {
	ScanStore
	TTLStore
}

type Txn interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
}

type ScanTxn interface {
	Txn
	Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error)
}

type TTLTxn interface {
	Txn
	Expire(ctx context.Context, key []byte, ttl int64) error
	PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error
}

type ScanTTLTxn interface {
	ScanTxn
	TTLTxn
}
