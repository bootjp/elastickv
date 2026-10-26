package filesystem

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestOperationRecordCodecProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		intent := rapid.SliceOfN(rapid.Byte(), 0, 1024).Draw(rt, "intent")
		sequence := rapid.Uint64Range(1, ^uint64(0)).Draw(rt, "sequence")
		id := OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: sequence}
		record := newOperationRecord(id, intent, 1024, 4096)
		raw, err := encodeOperationRecord(record)
		require.NoError(rt, err)
		decoded, err := decodeOperationRecord(raw)
		require.NoError(rt, err)
		require.Equal(rt, record, decoded)

		corrupt := append([]byte(nil), raw...)
		corrupt[4]++
		_, err = decodeOperationRecord(corrupt)
		require.ErrorIs(rt, err, ErrOperationRecordCorrupt)
	})
}

func TestOperationRecordCodecPreservesGenerationFence(t *testing.T) {
	id := OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: 1}
	record := newOperationRecord(id, []byte("write"), 128, 1024)
	record.AttemptID = [16]byte{4}
	record.Generation = 2
	record.FencedAttempts = [][16]byte{{3}}
	record.Reservation += operationFenceReservation
	raw, err := encodeOperationRecord(record)
	require.NoError(t, err)
	decoded, err := decodeOperationRecord(raw)
	require.NoError(t, err)
	require.Equal(t, record, decoded)

	decoded.FencedAttempts = append(decoded.FencedAttempts, [16]byte{3})
	_, err = encodeOperationRecord(decoded)
	require.ErrorIs(t, err, ErrOperationRecordCorrupt)

	legacy := newOperationRecord(id, []byte("write"), 128, 1024)
	legacy.RecordIsPrimary = false
	raw, err = encodeOperationRecord(legacy)
	require.NoError(t, err)
	raw[4] = operationRecordLegacyVersion
	decoded, err = decodeOperationRecord(raw)
	require.NoError(t, err)
	require.False(t, decoded.RecordIsPrimary)
}

func TestOperationSessionCodecProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		owner := rapid.StringMatching(`[a-z]{1,16}`).Draw(rt, "owner")
		watermark := rapid.Uint64Range(0, ^uint64(0)-1).Draw(rt, "watermark")
		session := newOperationSession(OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}, owner)
		session.RejectBefore = watermark
		session.AckGaps = []uint64{watermark + 1}
		raw, err := encodeOperationSession(session)
		require.NoError(rt, err)
		decoded, err := decodeOperationSession(raw)
		require.NoError(rt, err)
		require.Equal(rt, session, decoded)

		session.AckGaps[0] = watermark
		_, err = encodeOperationSession(session)
		require.ErrorIs(rt, err, ErrOperationRecordCorrupt)
	})
}
