package store

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/encryption"
)

// CounterNonceFactory is a test-only NonceFactory that produces the
// design §4.1 deterministic nonce shape (`node_id ‖ local_epoch ‖
// write_count`) without the writer-registry round-trip Stage 7
// brings. Production wiring uses the registry-backed factory; this
// implementation is only safe for tests where the caller controls
// every node_id / local_epoch combination.
//
// Exposed (vs. living in a *_test.go file) so the encryption
// integration tests in other packages can build on the same
// implementation without re-deriving the byte layout. It is
// nevertheless test-grade — the doc comment on NonceFactory
// emphasises that production callers MUST guarantee
// (node_id, local_epoch, write_count) uniqueness.
type CounterNonceFactory struct {
	nodeID     uint16
	localEpoch uint16
	writes     atomic.Uint64
}

// NewCounterNonceFactory constructs a CounterNonceFactory pinned to
// the given (nodeID, localEpoch). write_count starts at 0 and
// monotonically increments on every Next().
func NewCounterNonceFactory(nodeID, localEpoch uint16) *CounterNonceFactory {
	return &CounterNonceFactory{nodeID: nodeID, localEpoch: localEpoch}
}

// Next produces the next 12-byte nonce. Layout matches design §4.1:
//
//	bytes 0-1   node_id     (big-endian uint16)
//	bytes 2-3   local_epoch (big-endian uint16)
//	bytes 4-11  write_count (big-endian uint64)
//
// Big-endian is chosen so a hex dump of consecutive nonces is
// human-readable as a counter; the AAD does NOT include the nonce
// bytes (the cipher composes the nonce into AES-GCM directly), so
// the byte order is internal to the factory.
func (f *CounterNonceFactory) Next() ([encryption.NonceSize]byte, error) {
	var n [encryption.NonceSize]byte
	binary.BigEndian.PutUint16(n[0:2], f.nodeID)
	binary.BigEndian.PutUint16(n[2:4], f.localEpoch)
	binary.BigEndian.PutUint64(n[4:12], f.writes.Add(1))
	return n, nil
}
