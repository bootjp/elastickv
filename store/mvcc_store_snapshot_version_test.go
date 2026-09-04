package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// snapshotHeaderVersion reads the layout version a snapshot advertises, which
// is what an older binary checks against its own accept list before it will
// restore from it.
func snapshotHeaderVersion(t *testing.T, raw []byte) uint32 {
	t.Helper()

	require.Greater(t, len(raw), len(mvccSnapshotMagic)+4)
	require.Equal(t, mvccSnapshotMagic[:], raw[:len(mvccSnapshotMagic)])
	return binary.LittleEndian.Uint32(raw[len(mvccSnapshotMagic) : len(mvccSnapshotMagic)+4])
}

// Each snapshot layout is its predecessor plus one trailing metadata field, so
// a replica must advertise the oldest layout that can carry the state it
// actually holds. Advertising the newest one unconditionally strands a
// not-yet-upgraded follower during a rolling upgrade: it rejects the snapshot
// as an unsupported version and cannot catch up, even on a cluster where no
// migration has ever run and the extra fields would all be empty.
func TestMVCCStoreSnapshotEmitsOldestSufficientVersion(t *testing.T) {
	t.Parallel()

	prefix := []byte("stage|")
	seedPromotion := func(t *testing.T, st MVCCStore) {
		t.Helper()
		promoter, ok := any(st).(MigrationPromoter)
		require.True(t, ok)
		require.NoError(t, st.PutAt(context.Background(), append(bytes.Clone(prefix), []byte("k")...), []byte("v"), 70, 0))
		promoted, err := promoter.PromoteVersions(context.Background(), PromoteVersionsOptions{
			JobID:       7,
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			MaxVersions: 10,
			TargetKey: func(staged []byte) ([]byte, bool) {
				return bytes.TrimPrefix(staged, prefix), bytes.HasPrefix(staged, prefix)
			},
		})
		require.NoError(t, err)
		require.True(t, promoted.Done)
	}
	seedImport := func(t *testing.T, st MVCCStore) {
		t.Helper()
		_, err := st.ImportVersions(context.Background(), ImportVersionsOptions{
			JobID:     7,
			BracketID: 3,
			BatchSeq:  1,
			Cursor:    []byte("c"),
			Versions:  []MVCCVersion{{Key: []byte("imported"), CommitTS: 50, Value: []byte("v50")}},
		})
		require.NoError(t, err)
	}

	for _, tc := range []struct {
		name string
		seed func(*testing.T, MVCCStore)
		want uint32
	}{
		{
			name: "no migration metadata stays on the original layout",
			want: mvccSnapshotVersionV1,
		},
		{
			name: "import acks and hlc floors need v2",
			seed: seedImport,
			want: mvccSnapshotVersionV2,
		},
		{
			name: "promotion state needs v3",
			seed: seedPromotion,
			want: mvccSnapshotVersionV3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := newTestMVCCStore(t)
			require.NoError(t, st.PutAt(ctx, []byte("base"), []byte("v1"), 10, 0))
			if tc.seed != nil {
				tc.seed(t, st)
			}

			snap, err := st.Snapshot()
			require.NoError(t, err)
			defer snap.Close()
			raw := snapshotBytes(t, snap)

			require.Equal(t, tc.want, snapshotHeaderVersion(t, raw))

			// Whatever version was chosen, the body must match it: restoring
			// through the real reader reproduces the data and the metadata.
			dst := newTestMVCCStore(t)
			require.NoError(t, dst.Restore(bytes.NewReader(raw)))
			got, err := dst.GetAt(ctx, []byte("base"), 10)
			require.NoError(t, err)
			require.Equal(t, []byte("v1"), got)
		})
	}
}

// The metadata a newer layout exists to carry must survive the round trip, so
// the version choice cannot be made by simply dropping fields.
func TestMVCCStoreSnapshotRoundTripsMigrationMetadataAtItsVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	_, err := st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("c"),
		Versions:  []MVCCVersion{{Key: []byte("imported"), CommitTS: 50, Value: []byte("v50")}},
	})
	require.NoError(t, err)

	snap, err := st.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	raw := snapshotBytes(t, snap)
	require.Equal(t, mvccSnapshotVersionV2, snapshotHeaderVersion(t, raw))

	dst := newTestMVCCStore(t)
	require.NoError(t, dst.Restore(bytes.NewReader(raw)))
	floor, err := dst.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(50), floor, "the hlc floor is why v2 exists; it must survive")

	// The recorded ack is what makes a replayed batch a duplicate rather than a
	// second application, so it has to survive the restore as well.
	result, err := dst.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("replayed"),
		Versions:  []MVCCVersion{{Key: []byte("imported"), CommitTS: 50, Value: []byte("v50")}},
	})
	require.NoError(t, err)
	require.True(t, result.Duplicate)
	require.Equal(t, []byte("c"), result.AckedCursor)
}
