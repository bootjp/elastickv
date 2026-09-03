package kv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestTSOStateMachineApplyHLCLeaseUpdatesCeiling(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(1_700_000_123_456)
	hlc := NewHLC()
	fsm := NewTSOStateMachine(hlc)

	result := fsm.Apply(marshalHLCLeaseRenew(ceilingMs))
	require.Nil(t, result)
	require.Equal(t, ceilingMs, hlc.PhysicalCeiling())
	require.Zero(t, hlc.Current())
	require.Equal(t, ceilingMs, fsm.ceilingMs.Load())
}

func TestTSOStateMachineApplyRejectsOutOfRangeHLCLease(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	clock.Observe(123)
	clock.SetPhysicalCeiling(456)
	fsm := NewTSOStateMachine(clock)

	err := requireTSOHaltError(t, fsm.Apply(marshalRawHLCLeaseRenew(uint64(maxHLCPhysicalMillis)+1)))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.Equal(t, uint64(123), clock.Current())
	require.Equal(t, int64(456), clock.PhysicalCeiling())
	require.Zero(t, fsm.ceilingMs.Load())
}

func TestTSOStateMachineApplyAllocationFloorAdvancesHLC(t *testing.T) {
	t.Parallel()

	ceilingMs := time.Now().Add(time.Hour).UnixMilli()
	hlc := NewHLC()
	fsm := NewTSOStateMachine(hlc)

	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(ceilingMs)))
	floor := tsoLeaseAllocationFloor(ceilingMs - 1)
	require.Zero(t, hlc.Current())

	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(floor)))
	require.Equal(t, floor, hlc.Current())

	base, err := hlc.NextBatchFenced(1)
	require.NoError(t, err)
	require.Greater(t, base, floor)
}

func TestTSOStateMachineRejectsNonLeaseEntry(t *testing.T) {
	t.Parallel()

	payload := make([]byte, hlcLeaseEntryLen)
	payload[0] = raftEncodeSingle

	err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
}

func TestTSOStateMachineRejectsLegacyEncryptionEntriesWithoutHalting(t *testing.T) {
	t.Parallel()

	registration := fsmwire.RegistrationPayload{DEKID: 1, FullNodeID: 2, LocalEpoch: 3}
	tests := []struct {
		name    string
		opcode  byte
		payload []byte
	}{
		{
			name:    "registration",
			opcode:  fsmwire.OpRegistration,
			payload: fsmwire.EncodeRegistration(registration),
		},
		{
			name:   "bootstrap",
			opcode: fsmwire.OpBootstrap,
			payload: fsmwire.EncodeBootstrap(fsmwire.BootstrapPayload{
				StorageDEKID:   1,
				WrappedStorage: []byte("storage"),
				RaftDEKID:      2,
				WrappedRaft:    []byte("raft"),
				BatchRegistry:  []fsmwire.RegistrationPayload{registration},
			}),
		},
		{
			name:   "rotation",
			opcode: fsmwire.OpRotation,
			payload: fsmwire.EncodeRotation(fsmwire.RotationPayload{
				SubTag:               fsmwire.RotateSubRotateDEK,
				DEKID:                4,
				Purpose:              fsmwire.PurposeStorage,
				Wrapped:              []byte("rotated"),
				ProposerRegistration: registration,
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := append([]byte{tc.opcode}, tc.payload...)
			result := NewTSOStateMachine(NewHLC()).Apply(entry)
			err, ok := result.(error)
			require.Truef(t, ok, "legacy control response type = %T, want ordinary error", result)
			require.ErrorIs(t, err, ErrTSOLegacyEncryptionEntryRejected)
			_, halts := result.(interface{ HaltApply() error })
			require.False(t, halts, "valid legacy control entry must not halt replay")
		})
	}
}

func TestTSOStateMachineHaltsMalformedLegacyEncryptionEntry(t *testing.T) {
	t.Parallel()

	err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply([]byte{fsmwire.OpRegistration}))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
}

func TestTSOStateMachineRejectsMalformedLease(t *testing.T) {
	t.Parallel()

	for _, payload := range [][]byte{
		{},
		{raftEncodeHLCLease},
		append([]byte{raftEncodeHLCLease}, make([]byte, hlcLeasePayloadLen+1)...),
	} {
		err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
		require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	}
}

func TestTSOStateMachineRejectsNonPositiveLeaseCeiling(t *testing.T) {
	t.Parallel()

	for _, ceilingMs := range []int64{0, -1} {
		err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply(marshalHLCLeaseRenew(ceilingMs)))
		require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	}
}

func TestTSOStateMachineRejectsMalformedAllocationFloor(t *testing.T) {
	t.Parallel()

	for _, payload := range [][]byte{
		[]byte(tsoAllocationFloorEnvelope),
		append([]byte(tsoAllocationFloorEnvelope), make([]byte, hlcLeasePayloadLen+1)...),
		marshalTSOAllocationFloor(0),
	} {
		err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
		require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	}
}

func TestTSOStateMachineRejectsBareEncryptionReservedAllocationFloor(t *testing.T) {
	t.Parallel()

	payload := make([]byte, hlcLeaseEntryLen)
	payload[0] = fsmwire.OpEncryptionMax
	binary.BigEndian.PutUint64(payload[1:], 42)
	err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
}

func TestTSOStateMachineAppliesOneWayCutoverMarker(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())
	require.False(t, fsm.CutoverActive())
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.True(t, fsm.CutoverActive())
	require.Nil(t, fsm.Apply(marshalTSOCutover()), "cutover replay must be idempotent")

	err := requireTSOHaltError(t, fsm.Apply(append([]byte(tsoCutoverEnvelope), 1)))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
}

func TestTSOStateMachineAppliesOneWayPhaseDMarker(t *testing.T) {
	t.Parallel()

	const floor = uint64(1234)
	fsm := NewTSOStateMachine(NewHLC())

	err := requireTSOHaltError(t, fsm.Apply(marshalTSOPhaseD(floor)))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.False(t, fsm.PhaseDActive())

	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(floor)))
	require.True(t, fsm.PhaseDActive())
	require.Equal(t, floor, fsm.PhaseDFloor())
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(floor)), "phase-D replay must be idempotent")

	err = requireTSOHaltError(t, fsm.Apply(marshalTSOPhaseD(floor+1)))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.ErrorContains(t, err, "floor changed")
	err = requireTSOHaltError(t, fsm.Apply([]byte(tsoPhaseDEnvelope)))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
}

func TestTSOStateMachineRejectsPhaseDFloorBelowExistingAllocation(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(100)))
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	err := requireTSOHaltError(t, fsm.Apply(marshalTSOPhaseD(99)))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.ErrorContains(t, err, "below allocation floor")
	require.False(t, fsm.PhaseDActive())
}

func TestTSOStateMachineRejectsInvalidCutoverSnapshotByte(t *testing.T) {
	t.Parallel()

	payload := make([]byte, tsoSnapshotV3Len)
	payload[tsoSnapshotV2Len] = 2
	err := NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.ErrorContains(t, err, "invalid cutover byte")
}

func TestTSOStateMachineRejectsInvalidPhaseDSnapshot(t *testing.T) {
	t.Parallel()

	payload := make([]byte, tsoSnapshotV4Len)
	payload[tsoSnapshotV2Len] = 1
	payload[tsoSnapshotV3Len] = 2
	err := NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.ErrorContains(t, err, "invalid phase-D byte")

	payload[tsoSnapshotV2Len] = 0
	payload[tsoSnapshotV3Len] = 1
	err = NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.ErrorContains(t, err, "phase-D active without cutover")

	payload[tsoSnapshotV3Len] = 0
	binary.BigEndian.PutUint64(payload[tsoSnapshotV3Len+1:], 1)
	err = NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	require.ErrorContains(t, err, "inactive phase-D has floor")
}

func TestTSOStateMachineNilHLCDoesNotPanic(t *testing.T) {
	t.Parallel()

	require.Nil(t, NewTSOStateMachine(nil).Apply(marshalHLCLeaseRenew(1_700_000_123_456)))
	require.Nil(t, NewTSOStateMachine(nil).Apply(marshalTSOAllocationFloor(1)))
}

func TestTSOStateMachineSnapshotWithNilHLCWritesZeroState(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(nil)
	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.EqualValues(t, tsoSnapshotV1Len, n)
	require.Equal(t, make([]byte, tsoSnapshotV1Len), buf.Bytes())
}

func TestTSOStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(1_700_000_654_321)
	sourceHLC := NewHLC()
	source := NewTSOStateMachine(sourceHLC)
	require.Nil(t, source.Apply(marshalHLCLeaseRenew(ceilingMs)))
	floor := tsoLeaseAllocationFloor(ceilingMs)
	require.Nil(t, source.Apply(marshalTSOAllocationFloor(floor)))
	require.Nil(t, source.Apply(marshalTSOCutover()))
	require.Nil(t, source.Apply(marshalTSOPhaseD(floor)))
	postPhaseDFloor := floor + 10
	require.Nil(t, source.Apply(marshalTSOAllocationFloor(postPhaseDFloor)))

	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.EqualValues(t, tsoSnapshotV4Len, n)
	require.Len(t, buf.Bytes(), tsoSnapshotV4Len)

	targetHLC := NewHLC()
	target := NewTSOStateMachine(targetHLC)
	require.NoError(t, target.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, ceilingMs, targetHLC.PhysicalCeiling())
	require.Equal(t, postPhaseDFloor, targetHLC.Current())
	require.True(t, target.CutoverActive())
	require.True(t, target.PhaseDActive())
	require.Equal(t, floor, target.PhaseDFloor())
}

func TestTSOStateMachineSnapshotUsesTSOOwnedCeiling(t *testing.T) {
	t.Parallel()

	const (
		tsoCeiling       = int64(1_000)
		unrelatedCeiling = int64(2_000)
	)
	sourceHLC := NewHLC()
	source := NewTSOStateMachine(sourceHLC)
	require.Nil(t, source.Apply(marshalHLCLeaseRenew(tsoCeiling)))
	sourceHLC.SetPhysicalCeiling(unrelatedCeiling)

	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	targetHLC := NewHLC()
	require.NoError(t, NewTSOStateMachine(targetHLC).Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, tsoCeiling, targetHLC.PhysicalCeiling())
	// The restored floor is reconstructed from the TSO-owned ceiling, not from
	// the unrelated HLC value the source clock was carrying.
	require.Equal(t, tsoLeaseAllocationFloor(tsoCeiling), targetHLC.Current())
	require.Less(t, targetHLC.Current(), tsoLeaseAllocationFloor(unrelatedCeiling))
}

func TestTSOStateMachineRestoreRejectsTruncatedSnapshot(t *testing.T) {
	t.Parallel()

	err := NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader([]byte{0x01, 0x02}))
	require.Error(t, err)
}

func TestTSOStateMachineRestoreRejectsOutOfRangeSnapshot(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	clock.Observe(123)
	clock.SetPhysicalCeiling(456)
	fsm := NewTSOStateMachine(clock)

	var buf [tsoSnapshotV1Len]byte
	binary.BigEndian.PutUint64(buf[:], uint64(maxHLCPhysicalMillis)+1)
	require.Error(t, fsm.Restore(bytes.NewReader(buf[:])))
	require.Equal(t, uint64(123), clock.Current())
	require.Equal(t, int64(456), clock.PhysicalCeiling())
	require.Zero(t, fsm.ceilingMs.Load())
}

func TestTSOStateMachineLegacySnapshotProbeRejectsEveryShortMagicPrefix(t *testing.T) {
	t.Parallel()

	for size := range len(hlcSnapshotMagic) {
		payload := append([]byte(nil), hlcSnapshotMagic[:size]...)
		legacy, err := hasLegacyKVFSMSnapshotHeader(bufio.NewReader(bytes.NewReader(payload)))
		require.NoError(t, err)
		require.False(t, legacy)
	}
}

func TestTSOStateMachineRestoreLegacySnapshotDerivesAllocationFloor(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(1_700_000_654_321)
	var buf [hlcLeasePayloadLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(ceilingMs))

	hlc := NewHLC()
	require.NoError(t, NewTSOStateMachine(hlc).Restore(bytes.NewReader(buf[:])))
	require.Equal(t, ceilingMs, hlc.PhysicalCeiling())
	require.Equal(t, tsoLeaseAllocationFloor(ceilingMs), hlc.Current())
}

func TestTSOStateMachineRestoresV3SnapshotWithoutPhaseD(t *testing.T) {
	t.Parallel()

	const (
		ceilingMs = int64(1_700_000_654_321)
		floor     = uint64(4567)
	)
	payload := make([]byte, tsoSnapshotV3Len)
	binary.BigEndian.PutUint64(payload[:hlcLeasePayloadLen], uint64(ceilingMs))
	binary.BigEndian.PutUint64(payload[hlcLeasePayloadLen:tsoSnapshotV2Len], floor)
	payload[tsoSnapshotV2Len] = 1

	fsm := NewTSOStateMachine(NewHLC())
	require.NoError(t, fsm.Restore(bytes.NewReader(payload)))
	require.Equal(t, floor, fsm.AllocationFloor())
	require.True(t, fsm.CutoverActive())
	require.False(t, fsm.PhaseDActive())
	require.Zero(t, fsm.PhaseDFloor())
}

func TestTSOStateMachineRestoreKeepsMonotonicCeiling(t *testing.T) {
	t.Parallel()

	const (
		higherCeiling = int64(2_000)
		lowerCeiling  = int64(1_000)
	)
	hlc := NewHLC()
	fsm := NewTSOStateMachine(hlc)
	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(higherCeiling)))
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(tsoLeaseAllocationFloor(higherCeiling))))

	var buf [tsoSnapshotV2Len]byte
	binary.BigEndian.PutUint64(buf[:], uint64(lowerCeiling))

	require.NoError(t, fsm.Restore(bytes.NewReader(buf[:])))
	require.Equal(t, higherCeiling, hlc.PhysicalCeiling())
	require.Equal(t, tsoLeaseAllocationFloor(higherCeiling), hlc.Current())

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var snapBuf bytes.Buffer
	n, err := snap.WriteTo(&snapBuf)
	require.NoError(t, err)
	// The floor is exactly the value a V1 reader reconstructs from the ceiling,
	// so the shorter layout carries it implicitly and round-trips unchanged.
	require.EqualValues(t, tsoSnapshotV1Len, n)
	require.Equal(t, uint64(higherCeiling), binary.BigEndian.Uint64(snapBuf.Bytes()[:hlcLeasePayloadLen]))

	roundTripClock := NewHLC()
	require.NoError(t, NewTSOStateMachine(roundTripClock).Restore(bytes.NewReader(snapBuf.Bytes())))
	require.Equal(t, higherCeiling, roundTripClock.PhysicalCeiling())
	require.Equal(t, tsoLeaseAllocationFloor(higherCeiling), roundTripClock.Current())
}

func TestTSOStateMachineRestoresLegacyKVFSMSnapshot(t *testing.T) {
	const ceilingMs = int64(1_700_000_123_456)
	tests := []struct {
		name string
		opts []FSMOption
	}{
		{name: "v1"},
		{name: "v2", opts: []FSMOption{WithCutoverSource(&staticCutoverSource{v: 42})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			legacyClock := NewHLC()
			legacyClock.SetPhysicalCeiling(ceilingMs)
			legacyFSM := NewKvFSMWithHLC(store.NewMVCCStore(), legacyClock, tc.opts...)
			legacySnapshot, err := legacyFSM.Snapshot()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, legacySnapshot.Close()) })

			var legacy bytes.Buffer
			_, err = legacySnapshot.WriteTo(&legacy)
			require.NoError(t, err)

			targetClock := NewHLC()
			target := NewTSOStateMachine(targetClock)
			require.NoError(t, target.Restore(bytes.NewReader(legacy.Bytes())))
			require.Equal(t, ceilingMs, targetClock.PhysicalCeiling())
			require.Equal(t, tsoLeaseAllocationFloor(ceilingMs), targetClock.Current())

			got, err := target.Snapshot()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, got.Close()) })
			var payload bytes.Buffer
			_, err = got.WriteTo(&payload)
			require.NoError(t, err)
			// The restored floor is exactly what a V1 reader reconstructs, so
			// the re-emitted snapshot stays on the layout the previous binary
			// can still read -- a node that caught up from an old leader must
			// not become unreadable to its remaining old peers.
			require.Len(t, payload.Bytes(), tsoSnapshotV1Len)
			require.Equal(t, uint64(ceilingMs), binary.BigEndian.Uint64(payload.Bytes()[:hlcLeasePayloadLen]))
		})
	}
}

func TestTSOStateMachineDrainsHeaderlessLegacyKVFSMSnapshot(t *testing.T) {
	t.Parallel()

	legacySnapshot, err := store.NewMVCCStore().Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, legacySnapshot.Close()) })

	var legacy bytes.Buffer
	_, err = legacySnapshot.WriteTo(&legacy)
	require.NoError(t, err)

	targetClock := NewHLC()
	target := NewTSOStateMachine(targetClock)
	require.NoError(t, target.Restore(bytes.NewReader(legacy.Bytes())))
	require.Zero(t, targetClock.PhysicalCeiling())
	require.Zero(t, targetClock.Current())
}

func TestTSOStateMachineRejectsUnknownLongHeaderlessSnapshotPayload(t *testing.T) {
	t.Parallel()

	legacy := bytes.Repeat([]byte("legacy-kvfsm-headerless-body"), 2)
	require.Greater(t, len(legacy), tsoSnapshotV4Len)

	targetClock := NewHLC()
	target := NewTSOStateMachine(targetClock)
	require.ErrorIs(t, target.Restore(bytes.NewReader(legacy)), ErrTSOStateMachineInvalidEntry)
	require.Zero(t, targetClock.PhysicalCeiling())
	require.Zero(t, targetClock.Current())
}

func TestTSOStateMachineSnapshotWriteWrapsShortWrite(t *testing.T) {
	t.Parallel()

	snap := &tsoFSMSnapshot{ceilingMs: 1}
	n, err := snap.WriteTo(shortTSOWriter{})
	require.EqualValues(t, tsoSnapshotV1Len-1, n)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestTSOStateMachineClassifiesOnlyFullLeaseEntriesAsVolatile(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())
	require.True(t, fsm.IsVolatileOnlyPayload(marshalHLCLeaseRenew(1_700_000_123_456)))
	require.True(t, fsm.IsVolatileOnlyPayload(marshalTSOAllocationFloor(1)))
	require.True(t, fsm.IsVolatileOnlyPayload(marshalTSOCutover()))
	require.True(t, fsm.IsVolatileOnlyPayload(marshalTSOPhaseD(1)))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeHLCLease}))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte(tsoAllocationFloorEnvelope)))
	require.False(t, fsm.IsVolatileOnlyPayload(append([]byte(tsoCutoverEnvelope), 1)))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte(tsoPhaseDEnvelope)))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeSingle}))
}

func requireTSOHaltError(t *testing.T, result any) error {
	t.Helper()

	if _, ok := result.(error); ok {
		t.Fatalf("expected HaltApply response, got plain error %T", result)
	}
	halt, ok := result.(interface{ HaltApply() error })
	require.Truef(t, ok, "expected HaltApply response, got %T", result)
	err := halt.HaltApply()
	require.Error(t, err)
	return err
}

func marshalRawHLCLeaseRenew(raw uint64) []byte {
	payload := []byte{raftEncodeHLCLease}
	var buf [hlcLeasePayloadLen]byte
	binary.BigEndian.PutUint64(buf[:], raw)
	return append(payload, buf[:]...)
}

type shortTSOWriter struct{}

func (shortTSOWriter) Write(p []byte) (int, error) {
	return len(p) - 1, nil
}

type recordingDurableStateObserver struct {
	mu    sync.Mutex
	calls [][2]bool
}

func (o *recordingDurableStateObserver) ObserveTSODurableState(cutoverActive, phaseDActive bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, [2]bool{cutoverActive, phaseDActive})
}

func (o *recordingDurableStateObserver) last() ([2]bool, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.calls) == 0 {
		return [2]bool{}, false
	}

	return o.calls[len(o.calls)-1], true
}

// A replica that applies the markers but never serves an allocation, and under
// the backward-compatible startup flags never runs a mode-reload loop, has no
// other path that would move these gauges off their pre-cutover values.
func TestTSOStateMachinePublishesDurableStateOnApply(t *testing.T) {
	t.Parallel()

	observer := &recordingDurableStateObserver{}
	fsm := NewTSOStateMachine(NewHLC(), WithTSODurableStateObserver(observer))

	floor := uint64(1 << 20)
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(floor)))
	require.Nil(t, fsm.Apply(marshalTSOCutover()))

	got, ok := observer.last()
	require.True(t, ok, "applying the cutover marker must publish durable state")
	require.Equal(t, [2]bool{true, false}, got)

	require.Nil(t, fsm.Apply(marshalTSOPhaseD(floor)))
	got, ok = observer.last()
	require.True(t, ok)
	require.Equal(t, [2]bool{true, true}, got, "phase-D apply must publish too")
}

// A replica that joins by snapshot never replays the marker entries.
func TestTSOStateMachinePublishesDurableStateOnRestore(t *testing.T) {
	t.Parallel()

	source := NewTSOStateMachine(NewHLC())
	floor := uint64(1 << 20)
	require.Nil(t, source.Apply(marshalTSOAllocationFloor(floor)))
	require.Nil(t, source.Apply(marshalTSOCutover()))
	require.Nil(t, source.Apply(marshalTSOPhaseD(floor)))
	snap, err := source.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	observer := &recordingDurableStateObserver{}
	restored := NewTSOStateMachine(NewHLC(), WithTSODurableStateObserver(observer))
	require.NoError(t, restored.Restore(io.NopCloser(&buf)))

	got, ok := observer.last()
	require.True(t, ok, "restoring a snapshot must publish durable state")
	require.Equal(t, [2]bool{true, true}, got)
}

// The observer is installed after the group is built, so markers applied before
// wiring must still be published at install time.
func TestTSOStateMachineSetObserverPublishesExistingState(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(1<<20)))
	require.Nil(t, fsm.Apply(marshalTSOCutover()))

	observer := &recordingDurableStateObserver{}
	fsm.SetDurableStateObserver(observer)

	got, ok := observer.last()
	require.True(t, ok, "installing the observer must publish current state")
	require.Equal(t, [2]bool{true, false}, got)
}

// The Phase-D floor is immutable once active -- applyPhaseDMarker halts apply on
// a marker that changes it -- so a snapshot carrying a lower floor is older than
// what this replica already applied. Regressing to it reclassifies every
// timestamp in between as post-Phase-D, and ValidateDurableTimestamp starts
// accepting values it had been rejecting.
func TestTSOStateMachineRestoreDoesNotRegressPhaseDFloor(t *testing.T) {
	t.Parallel()

	ceilingMs := time.Now().Add(time.Hour).UnixMilli()
	oldFloor := tsoLeaseAllocationFloor(ceilingMs)
	newFloor := oldFloor + 1_000

	// A snapshot taken while the group was still at the older floor.
	source := NewTSOStateMachine(NewHLC())
	require.Nil(t, source.Apply(marshalHLCLeaseRenew(ceilingMs)))
	require.Nil(t, source.Apply(marshalTSOAllocationFloor(oldFloor)))
	require.Nil(t, source.Apply(marshalTSOCutover()))
	require.Nil(t, source.Apply(marshalTSOPhaseD(oldFloor)))
	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	// A replica that has already applied the higher floor.
	target := NewTSOStateMachine(NewHLC())
	require.Nil(t, target.Apply(marshalHLCLeaseRenew(ceilingMs)))
	require.Nil(t, target.Apply(marshalTSOAllocationFloor(newFloor)))
	require.Nil(t, target.Apply(marshalTSOCutover()))
	require.Nil(t, target.Apply(marshalTSOPhaseD(newFloor)))
	require.Equal(t, newFloor, target.PhaseDFloor())

	require.NoError(t, target.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, newFloor, target.PhaseDFloor(),
		"the older snapshot must not pull the floor back down")
	require.True(t, target.PhaseDActive())
}

// A snapshot carrying a higher floor than this replica has applied still moves
// the floor forward.
func TestTSOStateMachineRestoreAdvancesPhaseDFloor(t *testing.T) {
	t.Parallel()

	ceilingMs := time.Now().Add(time.Hour).UnixMilli()
	oldFloor := tsoLeaseAllocationFloor(ceilingMs)
	newFloor := oldFloor + 1_000

	source := NewTSOStateMachine(NewHLC())
	require.Nil(t, source.Apply(marshalHLCLeaseRenew(ceilingMs)))
	require.Nil(t, source.Apply(marshalTSOAllocationFloor(newFloor)))
	require.Nil(t, source.Apply(marshalTSOCutover()))
	require.Nil(t, source.Apply(marshalTSOPhaseD(newFloor)))
	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	target := NewTSOStateMachine(NewHLC())
	require.Nil(t, target.Apply(marshalHLCLeaseRenew(ceilingMs)))
	require.Nil(t, target.Apply(marshalTSOAllocationFloor(oldFloor)))
	require.Nil(t, target.Apply(marshalTSOCutover()))
	require.Nil(t, target.Apply(marshalTSOPhaseD(oldFloor)))

	require.NoError(t, target.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, newFloor, target.PhaseDFloor())
}
