package kv

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

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
}

func TestTSOStateMachineApplyAllocationFloorAdvancesHLC(t *testing.T) {
	t.Parallel()

	ceilingMs := time.Now().Add(time.Hour).UnixMilli()
	hlc := NewHLC()
	fsm := NewTSOStateMachine(hlc)

	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(ceilingMs)))
	floor := tsoLeaseAllocationFloor(ceilingMs)
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
		{raftEncodeTSOAllocationFloor},
		append([]byte{raftEncodeTSOAllocationFloor}, make([]byte, hlcLeasePayloadLen+1)...),
		marshalTSOAllocationFloor(0),
	} {
		err := requireTSOHaltError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
		require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	}
}

func TestTSOStateMachineNilHLCDoesNotPanic(t *testing.T) {
	t.Parallel()

	require.Nil(t, NewTSOStateMachine(nil).Apply(marshalHLCLeaseRenew(1_700_000_123_456)))
	require.Nil(t, NewTSOStateMachine(nil).Apply(marshalTSOAllocationFloor(1)))
}

func TestTSOStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(1_700_000_654_321)
	sourceHLC := NewHLC()
	source := NewTSOStateMachine(sourceHLC)
	require.Nil(t, source.Apply(marshalHLCLeaseRenew(ceilingMs)))
	floor := tsoLeaseAllocationFloor(ceilingMs)
	require.Nil(t, source.Apply(marshalTSOAllocationFloor(floor)))

	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.EqualValues(t, tsoSnapshotV2Len, n)
	require.Len(t, buf.Bytes(), tsoSnapshotV2Len)

	targetHLC := NewHLC()
	target := NewTSOStateMachine(targetHLC)
	require.NoError(t, target.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, ceilingMs, targetHLC.PhysicalCeiling())
	require.Equal(t, floor, targetHLC.Current())
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
	require.Zero(t, targetHLC.Current())
}

func TestTSOStateMachineRestoreRejectsTruncatedSnapshot(t *testing.T) {
	t.Parallel()

	err := NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader([]byte{0x01, 0x02}))
	require.Error(t, err)
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
	require.EqualValues(t, tsoSnapshotV2Len, n)
	require.Equal(t, uint64(higherCeiling), binary.BigEndian.Uint64(snapBuf.Bytes()[:hlcLeasePayloadLen]))
	require.Equal(t, tsoLeaseAllocationFloor(higherCeiling), binary.BigEndian.Uint64(snapBuf.Bytes()[hlcLeasePayloadLen:]))
}

func TestTSOStateMachineClassifiesOnlyFullLeaseEntriesAsVolatile(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())
	require.True(t, fsm.IsVolatileOnlyPayload(marshalHLCLeaseRenew(1_700_000_123_456)))
	require.True(t, fsm.IsVolatileOnlyPayload(marshalTSOAllocationFloor(1)))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeHLCLease}))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeTSOAllocationFloor}))
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
