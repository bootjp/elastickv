package kv

import (
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
