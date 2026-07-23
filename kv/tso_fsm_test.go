package kv

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTSOStateMachineApplyHLCLeaseUpdatesCeiling(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(9_999_999_999_999)
	clock := NewHLC()
	fsm := NewTSOStateMachine(clock)

	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(ceilingMs)))
	require.Equal(t, ceilingMs, clock.PhysicalCeiling())
	require.Equal(t, tsoCeilingMaxTimestamp(ceilingMs), clock.Current())
	require.Equal(t, ceilingMs, fsm.committedCeiling())
}

func TestTSOStateMachineApplyHLCLeaseExhaustsCommittedCeiling(t *testing.T) {
	t.Parallel()

	ceilingMs := time.Now().Add(time.Hour).UnixMilli()
	clock := NewHLC()
	fsm := NewTSOStateMachine(clock)

	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(ceilingMs)))
	_, err := clock.NextBatchFenced(1)
	require.ErrorIs(t, err, ErrCeilingExpired)
	require.Equal(t, uint64(1), clock.NextFencedRejections())
}

func TestTSOStateMachineApplyIgnoresNonLeasePayload(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	fsm := NewTSOStateMachine(clock)

	require.Nil(t, fsm.Apply([]byte{raftEncodeSingle}))
	require.Zero(t, clock.PhysicalCeiling())
}

func TestTSOStateMachineRejectsMalformedHLCLease(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())

	requireApplyError(t, fsm.Apply([]byte{raftEncodeHLCLease}))
	requireApplyError(t, fsm.Apply(append([]byte{raftEncodeHLCLease}, make([]byte, hlcLeasePayloadLen+1)...)))
}

func TestTSOStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ceilingMs := time.Now().Add(time.Hour).UnixMilli()
	sourceClock := NewHLC()
	source := NewTSOStateMachine(sourceClock)
	require.Nil(t, source.Apply(marshalHLCLeaseRenew(ceilingMs)))

	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.EqualValues(t, 8, n)
	require.Len(t, buf.Bytes(), 8)

	restoredClock := NewHLC()
	restored := NewTSOStateMachine(restoredClock)
	require.NoError(t, restored.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, ceilingMs, restoredClock.PhysicalCeiling())
	require.Equal(t, tsoCeilingMaxTimestamp(ceilingMs), restoredClock.Current())
	require.Equal(t, ceilingMs, restored.committedCeiling())
}

func TestTSOStateMachineSnapshotUsesCommittedCeiling(t *testing.T) {
	t.Parallel()

	committedCeilingMs := time.Now().Add(time.Hour).UnixMilli()
	unrelatedCeilingMs := committedCeilingMs + int64(time.Hour/time.Millisecond)
	sourceClock := NewHLC()
	source := NewTSOStateMachine(sourceClock)

	require.Nil(t, source.Apply(marshalHLCLeaseRenew(committedCeilingMs)))
	sourceClock.SetPhysicalCeiling(unrelatedCeilingMs)

	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	restoredClock := NewHLC()
	restored := NewTSOStateMachine(restoredClock)
	require.NoError(t, restored.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, committedCeilingMs, restoredClock.PhysicalCeiling())
	require.Equal(t, tsoCeilingMaxTimestamp(committedCeilingMs), restoredClock.Current())
}

func TestTSOStateMachineSnapshotWithNilHLCWritesZero(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(nil)
	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 8), buf.Bytes())
}

func TestTSOStateMachineRestoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())

	require.Error(t, fsm.Restore(bytes.NewReader(nil)))
	require.Error(t, fsm.Restore(bytes.NewReader(make([]byte, 9))))
}

func TestTSOStateMachineClassifiesHLCLeaseAsVolatileOnly(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())

	require.True(t, fsm.IsVolatileOnlyPayload(marshalHLCLeaseRenew(1)))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeSingle}))
	require.False(t, fsm.IsVolatileOnlyPayload(nil))
}

func requireApplyError(t *testing.T, got any) {
	t.Helper()

	err, ok := got.(error)
	require.True(t, ok)
	require.Error(t, err)
}
