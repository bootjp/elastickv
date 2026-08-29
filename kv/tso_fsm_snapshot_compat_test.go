package kv

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// restoreLikeLegacyTSOReader models the group-0 reader that shipped before the
// allocation floor: it reads exactly 8 bytes and rejects anything longer as
// trailing bytes. A rolling upgrade puts one of these on the receiving end of
// an upgraded leader's snapshot.
func restoreLikeLegacyTSOReader(t *testing.T, raw []byte) error {
	t.Helper()

	r := bytes.NewReader(raw)
	buf := make([]byte, hlcLeasePayloadLen)
	if _, err := r.Read(buf); err != nil {
		return err
	}
	var extra [1]byte
	n, _ := r.Read(extra[:])
	if n != 0 {
		return errors.New("tso fsm: restore snapshot: trailing bytes")
	}
	return nil
}

func snapshotBytesFor(t *testing.T, snap *tsoFSMSnapshot) []byte {
	t.Helper()

	var buf bytes.Buffer
	_, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}

// Every snapshot layout is its predecessor plus one trailing field, so the FSM
// must advertise the shortest one that can carry its state. While group 0
// carries nothing but HLC lease ceilings -- the whole rolling window -- that is
// the 8-byte form the previous binary restores. Emitting the longer form there
// strands a not-yet-upgraded follower that needs a snapshot after log
// compaction, which on a three-node group 0 puts quorum at risk.
func TestTSOFSMSnapshotEmitsShortestSufficientLayout(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		snap    tsoFSMSnapshot
		wantLen int
	}{
		{
			name:    "lease ceilings only stay on the legacy layout",
			snap:    tsoFSMSnapshot{ceilingMs: 1_700_000_000_000},
			wantLen: tsoSnapshotV1Len,
		},
		{
			// Restoring a pre-allocation-floor snapshot leaves the floor at
			// exactly the value a V1 reader reconstructs from the ceiling. A
			// node that caught up from an old leader must not become
			// unreadable to its remaining old peers, so that case stays on V1.
			name: "a lease-derived floor still fits the legacy layout",
			snap: tsoFSMSnapshot{
				ceilingMs:       1_700_000_000_000,
				allocationFloor: tsoLeaseAllocationFloor(1_700_000_000_000),
			},
			wantLen: tsoSnapshotV1Len,
		},
		{
			name:    "a real allocation floor needs v2",
			snap:    tsoFSMSnapshot{ceilingMs: 1_700_000_000_000, allocationFloor: 99},
			wantLen: tsoSnapshotV2Len,
		},
		{
			name:    "cutover needs v3",
			snap:    tsoFSMSnapshot{ceilingMs: 1_700_000_000_000, allocationFloor: 99, cutoverActive: true},
			wantLen: tsoSnapshotV3Len,
		},
		{
			name: "phase D needs v4",
			snap: tsoFSMSnapshot{
				ceilingMs: 1_700_000_000_000, allocationFloor: 99,
				cutoverActive: true, phaseDActive: true, phaseDFloor: 100,
			},
			wantLen: tsoSnapshotV4Len,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw := snapshotBytesFor(t, &tc.snap)
			require.Len(t, raw, tc.wantLen)

			// Whatever layout was chosen, this binary must round-trip it.
			var restored TSOStateMachine
			require.NoError(t, restored.Restore(bytes.NewReader(raw)))
			require.Equal(t, tc.snap.ceilingMs, restored.ceilingMs.Load())
			require.Equal(t, tc.snap.cutoverActive, restored.cutoverActive.Load())
			require.Equal(t, tc.snap.phaseDActive, restored.phaseDActive.Load())
		})
	}
}

// The point of staying on the legacy layout is that the previous binary can
// still read it.
func TestTSOFSMLegacyWindowSnapshotRestoresOnTheOldReader(t *testing.T) {
	t.Parallel()

	legacy := snapshotBytesFor(t, &tsoFSMSnapshot{ceilingMs: 1_700_000_000_000})
	require.NoError(t, restoreLikeLegacyTSOReader(t, legacy))

	// A floor restored from a pre-allocation-floor snapshot is reconstructible
	// from the ceiling, so it stays readable too.
	derived := snapshotBytesFor(t, &tsoFSMSnapshot{
		ceilingMs:       1_700_000_000_000,
		allocationFloor: tsoLeaseAllocationFloor(1_700_000_000_000),
	})
	require.NoError(t, restoreLikeLegacyTSOReader(t, derived))

	// Once the cluster actually commits an allocation floor it has left the
	// compatibility window, and the longer layout is expected to be rejected.
	withFloor := snapshotBytesFor(t, &tsoFSMSnapshot{ceilingMs: 1_700_000_000_000, allocationFloor: 99})
	require.Error(t, restoreLikeLegacyTSOReader(t, withFloor))
}
