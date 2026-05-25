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
	// The latch is permanent: every subsequent call must also fail
	// closed with ErrWriteCountExhausted rather than resuming from the
	// recycled low write_count range (which would reuse nonces already
	// emitted this load). Recovery is a restart, which bumps
	// local_epoch.
	for i := 0; i < 3; i++ {
		if _, err := f.Next(); !errors.Is(err, ErrWriteCountExhausted) {
			t.Fatalf("Next #%d after wrap: err = %v, want sticky ErrWriteCountExhausted", i, err)
		}
	}
}
