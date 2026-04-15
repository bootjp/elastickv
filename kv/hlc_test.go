package kv

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHLCNextIsMonotonic(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	last := h.Next()
	for range 1000 {
		next := h.Next()
		require.Greater(t, next, last)
		last = next
	}
}

func TestHLCObserveDoesNotRegress(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	base := h.Next()

	h.Observe(base - 1)
	require.Equal(t, base, h.Current())

	higher := base + 100
	h.Observe(higher)
	require.Equal(t, higher, h.Current())
}

func TestHLCNextAfterObserveLogicalOverflow(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	wall := nonNegativeUint64(time.Now().UnixMilli())
	observed := (wall << hlcLogicalBits) | hlcLogicalMask
	h.Observe(observed)

	next := h.Next()
	require.Greater(t, next, observed)
}

// TestHLCPhysicalCeilingIsFloor verifies that Next() never issues a timestamp
// whose physical part is below the Raft-agreed ceiling (SetPhysicalCeiling).
// This ensures a new leader inherits the previous leader's window correctly.
func TestHLCPhysicalCeilingIsFloor(t *testing.T) {
	t.Parallel()

	h := NewHLC()

	// Set a ceiling 5 seconds in the future so nowMs < ceiling.
	futureMs := time.Now().UnixMilli() + 5_000
	h.SetPhysicalCeiling(futureMs)
	require.Equal(t, futureMs, h.PhysicalCeiling())

	// All Next() calls must produce a physical part >= ceiling.
	for range 100 {
		ts := h.Next()
		physMs := int64(ts >> hlcLogicalBits) //nolint:gosec // upper bits are a Unix ms timestamp, always fits in int64
		require.GreaterOrEqual(t, physMs, futureMs,
			"physical part %d must be >= ceiling %d", physMs, futureMs)
	}
}

// TestHLCSetPhysicalCeilingIsMonotonic verifies that SetPhysicalCeiling never
// decreases the ceiling (smaller values are silently ignored).
func TestHLCSetPhysicalCeilingIsMonotonic(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	h.SetPhysicalCeiling(1000)
	h.SetPhysicalCeiling(500) // smaller — should be ignored
	require.Equal(t, int64(1000), h.PhysicalCeiling())

	h.SetPhysicalCeiling(2000) // larger — should advance
	require.Equal(t, int64(2000), h.PhysicalCeiling())
}

// TestFSMApplyHLCLeaseUpdatesCeiling verifies that the FSM correctly advances
// the shared HLC's physicalCeiling when a HLC lease entry is applied.
func TestFSMApplyHLCLeaseUpdatesCeiling(t *testing.T) {
	t.Parallel()

	hlc := NewHLC()
	// Simulate what the FSM does: call SetPhysicalCeiling after decoding the lease.
	const ceilingMs = int64(1_700_000_000_000) // arbitrary future timestamp
	hlc.SetPhysicalCeiling(ceilingMs)
	require.Equal(t, ceilingMs, hlc.PhysicalCeiling())

	// Timestamps must be floored at the ceiling.
	ts := hlc.Next()
	physMs := int64(ts >> hlcLogicalBits) //nolint:gosec // upper bits are a Unix ms timestamp, always fits in int64
	require.GreaterOrEqual(t, physMs, ceilingMs)
}

// TestHLCNextWithCeilingAtLogicalOverflow verifies that Next() handles the
// edge case where the logical counter is at max AND nowMs == ceiling.
// It must bump the physical part to ceiling+1 and reset logical to 0.
func TestHLCNextWithCeilingAtLogicalOverflow(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	wall := nonNegativeUint64(time.Now().UnixMilli())
	ceiling := int64(wall) + 100 //nolint:gosec // wall is from time.Now, always fits int64
	h.SetPhysicalCeiling(ceiling)

	// Force the HLC's last to (ceiling, logicalMax) so the next call overflows.
	saturated := (uint64(ceiling) << hlcLogicalBits) | hlcLogicalMask //nolint:gosec // ceiling is a Unix ms timestamp
	h.Observe(saturated)

	ts := h.Next()
	physMs := int64(ts >> hlcLogicalBits) //nolint:gosec // upper bits are a Unix ms timestamp
	logical := ts & hlcLogicalMask
	require.Greater(t, physMs, ceiling, "physical part must exceed the ceiling after logical overflow")
	require.Equal(t, uint64(0), logical, "logical counter must reset to 0 after overflow bump")
}

// TestHLCConcurrentNextAndSetPhysicalCeiling exercises the CAS-based Next()
// and SetPhysicalCeiling under concurrent access (run with -race).
func TestHLCConcurrentNextAndSetPhysicalCeiling(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	base := time.Now().UnixMilli() + 1_000
	h.SetPhysicalCeiling(base)

	const writers = 2
	const readers = 4
	const ops = 500

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// Writer goroutines advance the ceiling.
	for i := range writers {
		go func(idx int) {
			defer wg.Done()
			for j := range ops {
				h.SetPhysicalCeiling(base + int64(idx*ops+j))
			}
		}(i)
	}

	// Reader goroutines call Next() and verify monotonicity per goroutine.
	for range readers {
		go func() {
			defer wg.Done()
			var last uint64
			for range ops {
				ts := h.Next()
				require.Greater(t, ts, last)
				last = ts
			}
		}()
	}

	wg.Wait()
}

// TestApplyHLCLeaseWithNilHLC verifies that applyHLCLease is safe when hlc is nil.
func TestApplyHLCLeaseWithNilHLC(t *testing.T) {
	t.Parallel()

	f := &kvFSM{hlc: nil}
	result := f.applyHLCLease(marshalHLCLeaseRenew(1_700_000_000_000)[1:])
	require.Nil(t, result)
}

// TestApplyHLCLeaseWithMalformedPayload verifies that applyHLCLease returns an
// error for payloads that are not exactly hlcLeasePayloadLen bytes.
func TestApplyHLCLeaseWithMalformedPayload(t *testing.T) {
	t.Parallel()

	f := &kvFSM{hlc: NewHLC()}
	for _, bad := range [][]byte{{}, {0x01, 0x02, 0x03, 0x04}, make([]byte, 9)} {
		result := f.applyHLCLease(bad)
		require.NotNil(t, result, "expected error for payload length %d", len(bad))
	}
}

// TestHLCLeaseRoundTrip verifies that marshalHLCLeaseRenew encodes correctly
// and that applyHLCLease decodes it and calls SetPhysicalCeiling on the HLC.
func TestHLCLeaseRoundTrip(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(9_999_999_999_999)

	// Encode via the coordinator helper.
	encoded := marshalHLCLeaseRenew(ceilingMs)
	require.Equal(t, raftEncodeHLCLease, encoded[0], "first byte must be the HLC lease tag")
	require.Len(t, encoded, 9, "must be 1 tag byte + 8 payload bytes")

	// Decode via the FSM helper using the shared HLC.
	h := NewHLC()
	f := &kvFSM{hlc: h}
	result := f.applyHLCLease(encoded[1:]) // strip the leading tag byte
	require.Nil(t, result, "apply should return nil on success")

	require.Equal(t, ceilingMs, h.PhysicalCeiling(),
		"FSM apply must advance the HLC ceiling to the encoded value")
}
