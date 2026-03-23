package store

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
)

var ErrKeyNotFound = errors.New("not found")
var ErrUnknownOp = errors.New("unknown op")
var ErrNotSupported = errors.New("not supported")
var ErrInvalidChecksum = errors.New("invalid checksum")
var ErrWriteConflict = errors.New("write conflict")
var ErrExpired = errors.New("expired")
var ErrReadTSCompacted = errors.New("read timestamp has been compacted")
var ErrSnapshotKeyTooLarge = errors.New("mvcc snapshot key too large")
var ErrSnapshotVersionCountTooLarge = errors.New("mvcc snapshot version count too large")
var ErrSnapshotValueTooLarge = errors.New("snapshot value too large")

type WriteConflictError struct {
	key []byte
}

func NewWriteConflictError(key []byte) error {
	return &WriteConflictError{key: bytes.Clone(key)}
}

func WriteConflictKey(err error) ([]byte, bool) {
	var conflictErr *WriteConflictError
	if !errors.As(err, &conflictErr) {
		return nil, false
	}
	return bytes.Clone(conflictErr.key), true
}

func (e *WriteConflictError) Error() string {
	return fmt.Sprintf("key: %s: %v", string(e.key), ErrWriteConflict)
}

func (e *WriteConflictError) Unwrap() error {
	return ErrWriteConflict
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// OpType describes a mutation kind.
type OpType int

const (
	OpTypePut OpType = iota
	OpTypeDelete
)

var Tombstone = []byte{0x00}

const scanResultCapacityLimit = 1024

func boundedScanResultCapacity(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > scanResultCapacityLimit {
		return scanResultCapacityLimit
	}
	return limit
}

// HybridClock provides monotonically increasing timestamps (HLC).
type HybridClock interface {
	Now() uint64
}

// RetentionController exposes the minimum timestamp still retained by a store
// after MVCC compaction. Reads older than this watermark may fail with
// ErrReadTSCompacted.
type RetentionController interface {
	MinRetainedTS() uint64
	SetMinRetainedTS(ts uint64)
}

// Snapshot streams a consistent point-in-time store image to a writer.
// Implementations may back this with a temp file or an engine-native snapshot.
type Snapshot interface {
	io.WriterTo
	io.Closer
}

// MVCCStore extends Store with multi-version concurrency control helpers.
// The interface is timestamp-explicit; callers must supply the snapshot or
// commit timestamp for every operation.
type MVCCStore interface {
	// GetAt returns the newest version whose commit timestamp is <= ts.
	GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error)
	// ExistsAt reports whether a visible, non-tombstone version exists at ts.
	ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error)
	// ScanAt returns versions visible at the given timestamp.
	ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error)
	// ReverseScanAt returns visible versions in descending key order for keys in [start, end).
	ReverseScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*KVPair, error)
	// PutAt commits a value at the provided commit timestamp and optional expireAt.
	PutAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error
	// DeleteAt commits a tombstone at the provided commit timestamp.
	DeleteAt(ctx context.Context, key []byte, commitTS uint64) error
	// PutWithTTLAt stores a value with a precomputed expireAt (HLC) at the given commit timestamp.
	PutWithTTLAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error
	// ExpireAt sets/renews TTL using a precomputed expireAt (HLC) at the given commit timestamp.
	ExpireAt(ctx context.Context, key []byte, expireAt uint64, commitTS uint64) error
	// LatestCommitTS returns the commit timestamp of the newest version.
	// The boolean reports whether the key has any version.
	LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error)
	// ApplyMutations atomically validates and appends the provided mutations.
	// It must return ErrWriteConflict if any key has a newer commit timestamp
	// than startTS.
	ApplyMutations(ctx context.Context, mutations []*KVPairMutation, startTS, commitTS uint64) error
	// LastCommitTS returns the highest commit timestamp applied on this node.
	LastCommitTS() uint64
	// Compact removes versions older than minTS that are no longer needed.
	Compact(ctx context.Context, minTS uint64) error
	Snapshot() (Snapshot, error)
	Restore(buf io.Reader) error
	Close() error
}

// KVPairMutation is a small helper struct for MVCC mutation application.
type KVPairMutation struct {
	Op    OpType
	Key   []byte
	Value []byte
	// ExpireAt is an HLC timestamp; 0 means no TTL.
	ExpireAt uint64
}

// Legacy transactional helper interfaces removed; callers should stage their own
// mutation batches and use ApplyMutations/PutAt/DeleteAt directly.
