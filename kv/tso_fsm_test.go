package kv

import (
	"bytes"
	"encoding/binary"
	"io"
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
	require.Zero(t, clock.Current())
	require.Equal(t, ceilingMs, fsm.committedCeiling())
}

func TestTSOStateMachineApplyHLCLeaseKeepsRenewedWindowAllocatable(t *testing.T) {
	t.Parallel()

	firstCeilingMs := time.Now().Add(time.Hour).UnixMilli()
	secondCeilingMs := firstCeilingMs + 100
	clock := NewHLC()
	fsm := NewTSOStateMachine(clock)

	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(firstCeilingMs)))
	first, err := clock.NextBatchFenced(1)
	require.NoError(t, err)
	require.EqualValues(t, firstCeilingMs, first>>hlcLogicalBits)

	require.Nil(t, fsm.Apply(marshalHLCLeaseRenew(secondCeilingMs)))
	require.Equal(t, tsoCeilingMaxTimestamp(firstCeilingMs), clock.Current())
	second, err := clock.NextBatchFenced(1)
	require.NoError(t, err)
	require.EqualValues(t, secondCeilingMs, second>>hlcLogicalBits)
	require.Equal(t, uint64(0), clock.NextFencedRejections())
}

func TestTSOStateMachineApplyRejectsOutOfRangeHLCLease(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	clock.Observe(123)
	clock.SetPhysicalCeiling(456)
	fsm := NewTSOStateMachine(clock)

	resp := fsm.Apply(marshalRawHLCLeaseRenew(uint64(maxHLCPhysicalMillis) + 1))
	requireApplyError(t, resp)
	require.Equal(t, uint64(123), clock.Current())
	require.Equal(t, int64(456), clock.PhysicalCeiling())
	require.Zero(t, fsm.committedCeiling())
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
	_, err = restoredClock.NextBatchFenced(1)
	require.ErrorIs(t, err, ErrCeilingExpired)
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

func TestTSOStateMachineRestoreRejectsOutOfRangeSnapshot(t *testing.T) {
	t.Parallel()

	clock := NewHLC()
	clock.Observe(123)
	clock.SetPhysicalCeiling(456)
	fsm := NewTSOStateMachine(clock)

	var buf [tsoSnapshotLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(maxHLCPhysicalMillis)+1)
	require.Error(t, fsm.Restore(bytes.NewReader(buf[:])))
	require.Equal(t, uint64(123), clock.Current())
	require.Equal(t, int64(456), clock.PhysicalCeiling())
	require.Zero(t, fsm.committedCeiling())
}

func TestTSOStateMachineRestoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())

	require.Error(t, fsm.Restore(bytes.NewReader(nil)))
	require.Error(t, fsm.Restore(bytes.NewReader(make([]byte, 9))))
}

func TestTSOStateMachineSnapshotWriteWrapsShortWrite(t *testing.T) {
	t.Parallel()

	snap := &tsoSnapshot{ceilingMs: 1}
	n, err := snap.WriteTo(shortTSOWriter{})
	require.EqualValues(t, tsoSnapshotLen-1, n)
	require.ErrorIs(t, err, io.ErrShortWrite)
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
