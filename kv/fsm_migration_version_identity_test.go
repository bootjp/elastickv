package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationVersionIdentityLegacyAndProtectedImport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm := &kvFSM{store: st}
	key := []byte("k")
	staged := distribution.MigrationStagedDataKey(1, key)
	version := &pb.MVCCVersion{Key: key, CommitTs: 10, Value: []byte("first")}
	req := &pb.ImportRangeVersionsRequest{JobId: 1, BracketId: 1, BatchSeq: 1, Versions: []*pb.MVCCVersion{version}}

	require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, fsm, req, 1))
	req.BatchSeq++
	version.Value = []byte("legacy replacement")
	require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, fsm, req, 2))
	got, err := st.GetAt(ctx, staged, 10)
	require.NoError(t, err)
	require.Equal(t, version.Value, got)

	fsm.SetApplyIndex(3)
	require.Nil(t, fsm.Apply(EncodeMigrationVersionIdentityActivation()))
	protected, err := MigrationVersionIdentityEnabled(ctx, st)
	require.NoError(t, err)
	require.True(t, protected)

	req.BatchSeq++
	version.Value = []byte("protected replacement")
	resp := applyMigrationVersionIdentityImport(t, fsm, req, 4)
	requireMigrationVersionConflict(t, resp)
	got, err = st.GetAt(ctx, staged, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy replacement"), got)

	// A new staged job must also respect the public key's existing version.
	require.NoError(t, st.PutAt(ctx, key, []byte("live"), 10, 0))
	req.JobId, req.BatchSeq = 2, 1
	resp = applyMigrationVersionIdentityImport(t, fsm, req, 5)
	requireMigrationVersionConflict(t, resp)
	_, err = st.GetAt(ctx, distribution.MigrationStagedDataKey(2, key), 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestMigrationVersionIdentityPromotionPreservesStageOnConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm := &kvFSM{store: st}
	key := []byte("k")
	staged := distribution.MigrationStagedDataKey(1, key)
	require.NoError(t, st.PutWithTTLAt(ctx, key, []byte("value"), 10, 20))
	require.NoError(t, st.PutWithTTLAt(ctx, staged, []byte("value"), 10, 21))

	fsm.SetApplyIndex(1)
	require.Nil(t, fsm.Apply(EncodeMigrationVersionIdentityActivation()))
	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 1})
	require.NoError(t, err)
	fsm.SetApplyIndex(2)
	resp := fsm.Apply(cmd)
	requireMigrationVersionConflict(t, resp)
	_, err = st.GetAt(ctx, key, 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	got, err := st.GetAt(ctx, staged, 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), got)
	reader, ok := st.(store.MigrationPromotionStateReader)
	require.True(t, ok)
	_, present, err := reader.MigrationPromotionState(ctx, 1)
	require.NoError(t, err)
	require.False(t, present)

	// Identical versions are a successful move and only the staged copy is removed.
	cmd, err = MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 2})
	require.NoError(t, err)
	duplicate := distribution.MigrationStagedDataKey(2, key)
	require.NoError(t, st.PutWithTTLAt(ctx, duplicate, []byte("value"), 10, 20))
	fsm.SetApplyIndex(3)
	require.IsType(t, store.PromoteVersionsResult{}, fsm.Apply(cmd))
	_, err = st.GetAt(ctx, duplicate, 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	got, err = st.GetAt(ctx, key, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), got)
}

func applyMigrationVersionIdentityImport(t *testing.T, fsm *kvFSM, req *pb.ImportRangeVersionsRequest, index uint64) any {
	t.Helper()
	cmd, err := MarshalMigrationImportCommand(req)
	require.NoError(t, err)
	fsm.SetApplyIndex(index)
	return fsm.Apply(cmd)
}

func TestMigrationVersionIdentityActivationSurvivesPebbleReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	fsm := &kvFSM{store: st}
	fsm.SetApplyIndex(1)
	resp := fsm.Apply(EncodeMigrationVersionIdentityActivation())
	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.NoError(t, st.Close())

	st, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	protected, err := MigrationVersionIdentityEnabled(ctx, st)
	require.NoError(t, err)
	require.True(t, protected)
	fsm = &kvFSM{store: st}

	req := &pb.ImportRangeVersionsRequest{
		JobId: 1, BracketId: 1, BatchSeq: 1,
		Versions: []*pb.MVCCVersion{
			{Key: []byte("same"), CommitTs: 10, Value: []byte("original")},
			{Key: []byte("same"), CommitTs: 10, Value: []byte("collision")},
		},
	}
	resp = applyMigrationVersionIdentityImport(t, fsm, req, 2)
	requireMigrationVersionConflict(t, resp)
	_, err = st.GetAt(ctx, distribution.MigrationStagedDataKey(1, []byte("same")), 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestMigrationVersionIdentityLegacyPromotionOverwrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm := &kvFSM{store: st}
	key := []byte("k")
	staged := distribution.MigrationStagedDataKey(1, key)
	require.NoError(t, st.PutAt(ctx, key, []byte("unknown to old tokens"), 10, 0))
	require.NoError(t, st.DeleteAt(ctx, staged, 10))
	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 1})
	require.NoError(t, err)
	fsm.SetApplyIndex(1)
	require.IsType(t, store.PromoteVersionsResult{}, fsm.Apply(cmd))
	_, err = st.GetAt(ctx, key, 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = st.GetAt(ctx, staged, 10)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestMigrationVersionIdentityProtectsObservedCASToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fsm, st := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, fsm)
	key := []byte("key")
	staged := distribution.MigrationStagedDataKey(1, key)
	require.NoError(t, st.PutAt(ctx, key, []byte("A"), 10, 0))
	require.NoError(t, st.PutAt(ctx, staged, []byte("B"), 10, 0))

	observed, err := store.GetVersionAt(ctx, st, key, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("A"), observed.Value)
	require.Equal(t, uint64(10), observed.TS)

	req := &pb.ImportRangeVersionsRequest{
		JobId: 2, BracketId: 1, BatchSeq: 1,
		Versions: []*pb.MVCCVersion{{Key: key, CommitTs: observed.TS, Value: []byte("C")}},
	}
	resp := applyMigrationVersionIdentityImport(t, fsm, req, 2)
	requireMigrationVersionConflict(t, resp)

	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 1})
	require.NoError(t, err)
	fsm.SetApplyIndex(3)
	requireMigrationVersionConflict(t, fsm.Apply(cmd))

	current, err := store.GetVersionAt(ctx, st, key, 10)
	require.NoError(t, err)
	require.Equal(t, observed, current)
	require.Nil(t, applyCASFrame(t, fsm, 4, casApplyRequest(10, 20, observed.TS, string(key), "updated")))
	current, err = store.GetVersionAt(ctx, st, key, 20)
	require.NoError(t, err)
	require.Equal(t, []byte("updated"), current.Value)
	require.Equal(t, uint64(20), current.TS)
}

func TestMigrationVersionIdentityDoesNotExposeMigratedTokens(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	shards, group := newStagedVisibilityShardStore(t)
	t.Cleanup(func() { require.NoError(t, shards.Close()) })
	group.ConfigureVersionReadAdmission(func(context.Context) error { return nil })
	fsm := &kvFSM{store: group.Store}
	fsm.SetApplyIndex(1)
	require.Nil(t, fsm.Apply(EncodeMigrationVersionIdentityActivation()))
	key := []byte("key")
	req := &pb.ImportRangeVersionsRequest{
		JobId: 9, BracketId: 1, BatchSeq: 1,
		Versions: []*pb.MVCCVersion{{Key: key, CommitTs: 90, Value: []byte("migrated")}},
	}
	require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, fsm, req, 2))
	value, err := shards.GetAt(ctx, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("migrated"), value)
	_, err = shards.GetVersionAt(ctx, key, 100)
	require.ErrorIs(t, err, store.ErrNotSupported)

	cmd, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 9})
	require.NoError(t, err)
	fsm.SetApplyIndex(3)
	require.IsType(t, store.PromoteVersionsResult{}, fsm.Apply(cmd))
	require.NoError(t, shards.engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 2,
		Routes: []distribution.RouteDescriptor{{
			RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive,
			MinWriteTSExclusive: 100,
		}},
	}))
	value, err = shards.GetAt(ctx, key, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("migrated"), value)
	_, err = shards.GetVersionAt(ctx, key, 100)
	require.ErrorIs(t, err, store.ErrNotSupported)
}

func requireMigrationVersionConflict(t *testing.T, resp any) {
	t.Helper()
	require.NoError(t, haltApplyOf(resp))
	err, ok := resp.(error)
	require.True(t, ok, "expected migration conflict, got %T", resp)
	require.ErrorIs(t, err, store.ErrMigrationVersionConflict)
}
