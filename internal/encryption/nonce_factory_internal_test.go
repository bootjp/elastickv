package encryption

import (
	"errors"
	"math"
	"testing"
)

// TestDeterministicNonceFactory_WriteCountOverflow drives the factory's
// internal counter to the uint64 ceiling so the next Next() call wraps
// to 0, and asserts it fails closed with ErrWriteCountExhausted rather
// than emitting a reused nonce. White-box (package encryption) because
// it must set the unexported atomic counter directly.
func TestDeterministicNonceFactory_WriteCountOverflow(t *testing.T) {
	t.Parallel()
	f := NewDeterministicNonceFactory(0xABCD, 0x0001)
	// Pre-saturate: the next Add(1) wraps MaxUint64 -> 0.
	f.writes.Store(math.MaxUint64)
	if _, err := f.Next(); !errors.Is(err, ErrWriteCountExhausted) {
		t.Fatalf("Next at saturation: err = %v, want ErrWriteCountExhausted", err)
	}
	// The wrapped value 0 must not have been emitted; a subsequent
	// call continues from 1 and is a normal nonce again (the counter
	// has wrapped, but in practice recovery is a restart — this just
	// confirms Next does not panic post-wrap).
	if _, err := f.Next(); err != nil {
		t.Fatalf("Next after wrap: unexpected err = %v", err)
	}
}
