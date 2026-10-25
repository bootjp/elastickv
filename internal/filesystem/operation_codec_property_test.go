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
