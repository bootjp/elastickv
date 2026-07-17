package kv

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

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
}

func TestTSOStateMachineRejectsNonLeaseEntry(t *testing.T) {
	t.Parallel()

	payload := make([]byte, hlcLeaseEntryLen)
	payload[0] = raftEncodeSingle

	err := requireTSOApplyError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
	require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
}

func TestTSOStateMachineRejectsMalformedLease(t *testing.T) {
	t.Parallel()

	for _, payload := range [][]byte{
		{},
		{raftEncodeHLCLease},
		append([]byte{raftEncodeHLCLease}, make([]byte, hlcLeasePayloadLen+1)...),
	} {
		err := requireTSOApplyError(t, NewTSOStateMachine(NewHLC()).Apply(payload))
		require.ErrorIs(t, err, ErrTSOStateMachineInvalidEntry)
	}
}

func TestTSOStateMachineNilHLCDoesNotPanic(t *testing.T) {
	t.Parallel()

	require.Nil(t, NewTSOStateMachine(nil).Apply(marshalHLCLeaseRenew(1_700_000_123_456)))
}

func TestTSOStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	const ceilingMs = int64(1_700_000_654_321)
	sourceHLC := NewHLC()
	sourceHLC.SetPhysicalCeiling(ceilingMs)
	source := NewTSOStateMachine(sourceHLC)

	snap, err := source.Snapshot()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.EqualValues(t, hlcLeasePayloadLen, n)
	require.Len(t, buf.Bytes(), hlcLeasePayloadLen)

	targetHLC := NewHLC()
	target := NewTSOStateMachine(targetHLC)
	require.NoError(t, target.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, ceilingMs, targetHLC.PhysicalCeiling())
}

func TestTSOStateMachineRestoreRejectsTruncatedSnapshot(t *testing.T) {
	t.Parallel()

	err := NewTSOStateMachine(NewHLC()).Restore(bytes.NewReader([]byte{0x01, 0x02}))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestTSOStateMachineRestoreKeepsMonotonicCeiling(t *testing.T) {
	t.Parallel()

	const (
		higherCeiling = int64(2_000)
		lowerCeiling  = int64(1_000)
	)
	hlc := NewHLC()
	hlc.SetPhysicalCeiling(higherCeiling)

	var buf [hlcLeasePayloadLen]byte
	binary.BigEndian.PutUint64(buf[:], uint64(lowerCeiling))

	require.NoError(t, NewTSOStateMachine(hlc).Restore(bytes.NewReader(buf[:])))
	require.Equal(t, higherCeiling, hlc.PhysicalCeiling())
}

func TestTSOStateMachineClassifiesOnlyFullLeaseEntriesAsVolatile(t *testing.T) {
	t.Parallel()

	fsm := NewTSOStateMachine(NewHLC())
	require.True(t, fsm.IsVolatileOnlyPayload(marshalHLCLeaseRenew(1_700_000_123_456)))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeHLCLease}))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeSingle}))
}

func requireTSOApplyError(t *testing.T, result any) error {
	t.Helper()

	err, ok := result.(error)
	require.Truef(t, ok, "expected error result, got %T", result)
	return err
}
