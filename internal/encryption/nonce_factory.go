package encryption

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

// DeterministicNonceFactory is the production §4.1 storage-envelope
// nonce source. It emits the 12-byte deterministic nonce
//
//	bytes 0-1   node_id     (big-endian uint16)
//	bytes 2-3   local_epoch (big-endian uint16)
//	bytes 4-11  write_count (big-endian uint64)
//
// with zero random bits — nonce uniqueness is by construction across
// (node, process-load, write). It satisfies the store.NonceFactory
// interface structurally (Next() ([NonceSize]byte, error)).
//
// Safety contract (see the parent encryption design §4.1):
//
//   - node_id is uint16(DeriveNodeID(--raftId)); cluster-wide 16-bit
//     uniqueness is enforced by the writer registry + the
//     ErrNodeIDCollision / membership-snapshot startup guards.
//   - local_epoch is pinned at construction from a value that was
//     bumped and fsync'd on THIS process load (BumpLocalEpoch). The
//     factory never advances the epoch; one factory instance == one
//     process load == one epoch.
//   - write_count is an atomic counter that resets to 0 each process
//     load. The reset is only safe BECAUSE local_epoch advanced, so
//     constructing this factory with an un-bumped epoch reused across
//     restarts is a correctness bug. Always pair NewDeterministicNonceFactory
//     with a fresh BumpLocalEpoch on the active storage DEK.
//
// This is the durable analogue of the test-only
// store.CounterNonceFactory: identical byte layout, but the epoch
// here carries the restart-safety guarantee the test factory lacks.
type DeterministicNonceFactory struct {
	nodeID     uint16
	localEpoch uint16
	writes     atomic.Uint64
	// exhausted latches true the first time write_count wraps past
	// 2^64. Once set, every subsequent Next() fails closed with
	// ErrWriteCountExhausted rather than resuming from the recycled
	// low write_count range. Without the latch, the call after the
	// wrap would emit write_count=1 again — a nonce already used this
	// load under the same (node_id, local_epoch).
	exhausted atomic.Bool
}

// NewDeterministicNonceFactory constructs a factory pinned to
// (nodeID, localEpoch). write_count starts at 0 and increments on
// every Next(). The caller is responsible for having bumped and
// fsync'd localEpoch for this process load before issuing any nonce.
func NewDeterministicNonceFactory(nodeID, localEpoch uint16) *DeterministicNonceFactory {
	return &DeterministicNonceFactory{nodeID: nodeID, localEpoch: localEpoch}
}

// Next returns the next 12-byte nonce. The write_count is
// pre-incremented (the first nonce of a process load carries
// write_count=1, not 0) so that no nonce ever carries the all-zero
// write_count — keeping the nonce space disjoint from any future
// scheme that might want write_count=0 as a sentinel. The atomic add
// makes Next safe for concurrent callers.
//
// Overflow: atomic.Uint64.Add wraps silently at 2^64. A wrap returns
// the value 0 (the pre-increment all-ones value plus one), which
// would recycle write_count=1.. under the same (node_id, local_epoch)
// — a catastrophic GCM nonce reuse. On the wrapping call Next latches
// the factory exhausted and returns ErrWriteCountExhausted; every
// subsequent call also returns ErrWriteCountExhausted (the latch is
// permanent — resuming from the recycled low range would reuse nonces
// already emitted this load). The boundary is unreachable in practice
// (2^64 writes per process load); recovery is a restart, which bumps
// local_epoch and resets write_count to a fresh, non-overlapping
// range.
func (f *DeterministicNonceFactory) Next() ([NonceSize]byte, error) {
	if f.exhausted.Load() {
		return [NonceSize]byte{}, errors.WithStack(ErrWriteCountExhausted)
	}
	wc := f.writes.Add(1)
	if wc == 0 {
		f.exhausted.Store(true)
		return [NonceSize]byte{}, errors.WithStack(ErrWriteCountExhausted)
	}
	var n [NonceSize]byte
	binary.BigEndian.PutUint16(n[0:2], f.nodeID)
	binary.BigEndian.PutUint16(n[2:4], f.localEpoch)
	binary.BigEndian.PutUint64(n[4:12], wc)
	return n, nil
}
