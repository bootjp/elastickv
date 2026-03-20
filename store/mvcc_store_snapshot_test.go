package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := newTestMVCCStore(t)
	require.NoError(t, src.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, src.PutAt(ctx, []byte("k2"), []byte("v2"), 20, 0))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	defer snap.Close()

	raw := snapshotBytes(t, snap)

	// Mutate source after snapshot so restore must reflect snapshot point-in-time.
	require.NoError(t, src.PutAt(ctx, []byte("k1"), []byte("v3"), 30, 0))

	dst := newTestMVCCStore(t)
	require.NoError(t, dst.Restore(bytes.NewReader(raw)))
	require.Equal(t, uint64(20), dst.LastCommitTS())

	v, err := dst.GetAt(ctx, []byte("k1"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)

	v, err = dst.GetAt(ctx, []byte("k2"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v)
}

func TestMVCCStore_RestoreRejectsInvalidChecksum(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 10, 0))

	snap, err := st.Snapshot()
	require.NoError(t, err)
	defer snap.Close()

	raw := snapshotBytes(t, snap)
	require.NotEmpty(t, raw)
	raw[len(raw)-1] ^= 0xFF

	require.ErrorIs(t, st.Restore(bytes.NewReader(raw)), ErrInvalidChecksum)
}

func TestMVCCStore_ApplyMutations_WriteConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 20, 0))

	err := st.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k"), Value: []byte("v2")},
	}, 10, 30)
	require.ErrorIs(t, err, ErrWriteConflict)
}

func TestMVCCStore_ApplyMutations_UnknownOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	err := st.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpType(99), Key: []byte("k"), Value: []byte("v")},
	}, 10, 20)
	require.ErrorIs(t, err, ErrUnknownOp)
}

func TestMVCCStore_RestoreLegacySnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Build a legacy gob+crc32 snapshot payload.
	legacy := mvccSnapshot{
		LastCommitTS:  42,
		MinRetainedTS: 10,
		Entries: []mvccSnapshotEntry{
			{
				Key: []byte("legacy-key"),
				Versions: []VersionedValue{
					{TS: 42, Value: []byte("legacy-value"), Tombstone: false, ExpireAt: 0},
				},
			},
		},
	}

	var payload bytes.Buffer
	require.NoError(t, gob.NewEncoder(&payload).Encode(legacy))

	checksum := crc32.ChecksumIEEE(payload.Bytes())
	var cs [4]byte
	binary.LittleEndian.PutUint32(cs[:], checksum)
	full := append(payload.Bytes(), cs[:]...)

	dst := newTestMVCCStore(t)
	require.NoError(t, dst.Restore(bytes.NewReader(full)))

	require.Equal(t, uint64(42), dst.LastCommitTS())

	v, err := dst.GetAt(ctx, []byte("legacy-key"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy-value"), v)
}

func snapshotBytes(t *testing.T, snap Snapshot) []byte {
	t.Helper()

	var buf bytes.Buffer
	_, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}
