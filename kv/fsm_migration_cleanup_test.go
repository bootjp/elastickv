package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestApplyMigrationCleanupCommandDeletesBoundedVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("old"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("new"), 20, 0))
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	cmd, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId:           1,
		Mode:            pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart:      []byte("a"),
		RangeEnd:        []byte("b"),
		MaxCommitTs:     10,
		MaxVersions:     16,
		MaxBytes:        1 << 20,
		MaxScannedBytes: 1 << 20,
		KeyFamily:       1,
	})
	require.NoError(t, err)
	result, ok := fsm.applyMigrationCleanup(ctx, cmd[1:]).(store.CleanupVersionsResult)
	require.True(t, ok)
	require.True(t, result.Done)
	require.Equal(t, uint64(1), result.DeletedRows)

	value, err := st.GetAt(ctx, []byte("a"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
	_, err = st.GetAt(ctx, []byte("a"), 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestVersionIdentityCleanupRejectsTombstoneRemovalWithDifferentRetainedHistory(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"retained", "compacted"} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			st, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
			require.True(t, ok)
			enableCASApplyForTest(t, fsm)
			key := []byte("a")
			require.NoError(t, st.PutAt(ctx, key, []byte("issued"), 10, 0))
			issued, err := store.GetVersionAt(ctx, st, key, 10)
			require.NoError(t, err)
			require.NoError(t, st.DeleteAt(ctx, key, 20))
			if name == "compacted" {
				require.NoError(t, st.Compact(ctx, 21))
			}
			cmd, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
				JobId: 7, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
				RangeStart: key, RangeEnd: nextScanCursor(key), MaxCommitTs: 20, MaxVersions: 1,
			})
			require.NoError(t, err)
			fsm.SetApplyIndex(2)
			requireCASRejection(t, fsm.Apply(cmd), store.ErrMigrationOwnership)
			requireCASProgress(t, fsm, 2)
			requireCASRejection(t, applyCASFrame(t, fsm, 3, casApplyRequest(25, 30, issued.TS, string(key), "resurrect")), ErrCASMismatch)
			requireCASProgress(t, fsm, 3)
			_, err = store.GetVersionAt(ctx, st, key, 30)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestVersionIdentityCleanupAllowsAbandonedStagedRows(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"memory", "pebble"} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			st := store.NewMVCCStore()
			if name == "pebble" {
				var err error
				st, err = store.NewPebbleStore(t.TempDir())
				require.NoError(t, err)
			}
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			fsm, ok := NewKvFSMWithHLC(st, nil).(*kvFSM)
			require.True(t, ok)
			enableCASApplyForTest(t, fsm)
			const jobID = 7
			key := []byte("a")
			require.NoError(t, st.PutAt(ctx, key, []byte("live"), 11, 0))
			require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, fsm, &pb.ImportRangeVersionsRequest{
				JobId: jobID, BracketId: 1, BatchSeq: 1, Versions: []*pb.MVCCVersion{{Key: key, CommitTs: 10, Value: []byte("staged")}},
			}, 2))
			// The retained staged head is older than the local read floor.
			require.NoError(t, st.Compact(ctx, 11))
			prefix := distribution.MigrationStagedDataKeyPrefix(jobID)
			cmd, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
				JobId: jobID, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS, KeyFamily: distribution.MigrationFamilyUser,
				RangeStart: prefix, RangeEnd: store.PrefixScanEnd(prefix), MaxCommitTs: 10,
			})
			require.NoError(t, err)
			fsm.SetApplyIndex(3)
			response := fsm.Apply(cmd)
			cleaned, ok := response.(store.CleanupVersionsResult)
			require.True(t, ok, "cleanup returned %v", response)
			require.True(t, cleaned.Done)
			require.Equal(t, uint64(1), cleaned.DeletedRows)
			if name == "pebble" {
				requireCASProgress(t, fsm, 3)
			}
			_, err = st.GetAt(ctx, distribution.MigrationStagedDataKey(jobID, key), 20)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			live, err := store.GetVersionAt(ctx, st, key, 20)
			require.NoError(t, err)
			require.Equal(t, store.VersionedValue{TS: 11, Value: []byte("live")}, live)
		})
	}
}
