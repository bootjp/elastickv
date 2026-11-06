package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftControlMetadataSnapshotRetention(t *testing.T) {
	for _, format := range []string{"memory", "native", "sst"} {
		t.Run(format, func(t *testing.T) {
			var source MVCCStore
			if format == "memory" {
				source = NewMVCCStore()
				t.Cleanup(func() { require.NoError(t, source.Close()) })
			} else {
				source = openSSTSnapshotTestStore(t, t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
			}
			control := raftMetadata(t, source)
			require.NoError(t, raftEntryApplier(t, source).ApplyRaftEntry(t.Context(), 11, func(ctx context.Context) error {
				for _, key := range []string{"P", "reservation", "R/11", "J/11", "D/learner", "tombstone/member"} {
					if err := control.StageRaftControlMetadata(ctx, []byte(key), []byte("original/"+key)); err != nil {
						return err
					}
				}
				return source.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "value"), nil, 10, 10, 11)
			}))
			require.NoError(t, source.Compact(t.Context(), 20))
			snapshot, err := source.Snapshot()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
			require.NoError(t, raftEntryApplier(t, source).ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error {
				return control.StageRaftControlMetadata(ctx, []byte("P"), []byte("after-snapshot"))
			}))
			var payload bytes.Buffer
			_, err = snapshot.WriteTo(&payload)
			require.NoError(t, err)
			if format == "memory" {
				require.Equal(t, mvccSnapshotVersionV8, binary.LittleEndian.Uint32(payload.Bytes()[8:12]))
			}
			var target MVCCStore
			if format == "memory" {
				target = NewMVCCStore()
				t.Cleanup(func() { require.NoError(t, target.Close()) })
			} else {
				target = openSSTSnapshotTestStore(t, t.TempDir())
			}
			require.NoError(t, target.Restore(bytes.NewReader(payload.Bytes())))
			for _, key := range []string{"P", "reservation", "R/11", "J/11", "D/learner", "tombstone/member"} {
				got, readErr := raftMetadata(t, target).ReadRaftControlMetadata(t.Context(), []byte(key))
				require.NoError(t, readErr)
				require.Equal(t, []byte("original/"+key), got)
			}
			require.Equal(t, uint64(10), target.LastCommitTS())
			if format != "memory" {
				index, present := raftEntryAppliedIndex(t, target)
				require.True(t, present)
				require.Equal(t, uint64(11), index)
			} else {
				// The portable memory codec must retain raw control rows when
				// converted to a native Pebble store as well.
				pebbleTarget := openSSTSnapshotTestStore(t, t.TempDir())
				require.NoError(t, pebbleTarget.Restore(bytes.NewReader(payload.Bytes())))
				got, readErr := pebbleTarget.ReadRaftControlMetadata(t.Context(), []byte("D/learner"))
				require.NoError(t, readErr)
				require.Equal(t, []byte("original/D/learner"), got)
				index, present := raftEntryAppliedIndex(t, pebbleTarget)
				require.True(t, present)
				require.Equal(t, uint64(11), index)
			}
			external, ok := target.(ExternalSnapshotRestorer)
			require.True(t, ok)
			require.ErrorIs(t, external.RestoreExternal(t.Context(), bytes.NewReader(payload.Bytes())), ErrExternalSnapshotControlState)
			got, readErr := raftMetadata(t, target).ReadRaftControlMetadata(t.Context(), []byte("P"))
			require.NoError(t, readErr)
			require.Equal(t, []byte("original/P"), got, "rejected external restore must not swap the live store")
		})
	}
}

func TestRaftControlMetadataEncryptedSnapshotAndReopen(t *testing.T) {
	fixture := newEncryptedStoreFixture(t, 1)
	api := raftMetadata(t, fixture.mvcc)
	value := []byte("protected control payload not stored in plaintext")
	require.NoError(t, raftEntryApplier(t, fixture.mvcc).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		return api.StageRaftControlMetadata(ctx, []byte("P"), value)
	}))
	physical, err := raftControlKey([]byte("P"))
	require.NoError(t, err)
	stored, closer, err := pebbleStoreApplied(t, fixture.mvcc).db.Get(physical)
	require.NoError(t, err)
	require.NotContains(t, string(stored), string(value))
	require.NoError(t, closer.Close())
	snapshot, err := fixture.mvcc.Snapshot()
	require.NoError(t, err)
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	fixture.closeIfOpen(t)
	fixture.reopen(t)
	got, err := raftMetadata(t, fixture.mvcc).ReadRaftControlMetadata(t.Context(), []byte("P"))
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.NoError(t, fixture.mvcc.Restore(bytes.NewReader(payload.Bytes())))
	got, err = raftMetadata(t, fixture.mvcc).ReadRaftControlMetadata(t.Context(), []byte("P"))
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.Zero(t, fixture.mvcc.LastCommitTS())
}

func TestRaftControlSnapshotBoundsAndCanonicalKeys(t *testing.T) {
	var payload bytes.Buffer
	require.NoError(t, binary.Write(&payload, binary.LittleEndian, uint64(1)))
	require.NoError(t, binary.Write(&payload, binary.LittleEndian, uint64(1)))
	require.NoError(t, binary.Write(&payload, binary.LittleEndian, ^uint64(0)))
	_, _, err := readRaftControlSnapshot(&payload, mvccSnapshotVersionV8)
	require.Error(t, err)
	payload.Reset()
	require.NoError(t, binary.Write(&payload, binary.LittleEndian, uint64(1)))
	require.NoError(t, binary.Write(&payload, binary.LittleEndian, uint64(2)))
	for range 2 {
		require.NoError(t, writeMVCCSnapshotBytes(&payload, []byte("P")))
		require.NoError(t, writeMVCCSnapshotBytes(&payload, []byte("value")))
	}
	_, _, err = readRaftControlSnapshot(&payload, mvccSnapshotVersionV8)
	require.Error(t, err)
}

func TestRaftControlMemorySnapshotTracksLaterInnerIndex(t *testing.T) {
	s := NewMVCCStore()
	defer s.Close()
	require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		return raftMetadata(t, s).StageRaftControlMetadata(ctx, []byte("P"), []byte("protected"))
	}))
	require.NoError(t, raftEntryApplier(t, s).ApplyRaftEntry(t.Context(), 8, func(ctx context.Context) error {
		return s.ApplyMutationsRaftAt(ctx, nil, nil, 10, 10, 8)
	}))
	snapshot, err := s.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	target := openSSTSnapshotTestStore(t, t.TempDir())
	require.NoError(t, target.Restore(&payload))
	index, present := raftEntryAppliedIndex(t, target)
	require.True(t, present)
	require.Equal(t, uint64(8), index)
	require.Equal(t, uint64(10), target.LastCommitTS())
}
