package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestVersionCASMoveToUnactivatedGroupAndBack(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	source, target := store.NewMVCCStore(), store.NewMVCCStore()
	sourceFSM, targetFSM := &kvFSM{store: source}, &kvFSM{store: target}
	enableCASApplyForTest(t, sourceFSM)
	key := []byte("key")
	require.NoError(t, source.PutAt(ctx, key, []byte("source"), 10, 0))
	routes := distribution.NewEngine()
	setOwner := func(version, groupID, floor uint64) {
		t.Helper()
		require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{
			Version: version, Routes: []distribution.RouteDescriptor{{
				RouteID: version, GroupID: groupID, State: distribution.RouteStateActive, MinWriteTSExclusive: floor,
			}},
		}))
	}
	setOwner(1, 1, 0)
	groups := map[uint64]*ShardGroup{1: {Store: source}, 2: {Store: target}}
	for _, group := range groups {
		group.ConfigureVersionReadAdmission(func(ctx context.Context) error {
			active, err := MigrationVersionIdentityEnabled(ctx, group.Store)
			if err == nil && !active {
				return ErrVersionCASUnavailable
			}
			return err
		})
	}
	shards := NewShardStore(routes, groups)
	t.Cleanup(func() { require.NoError(t, shards.Close()) })
	coordinator := NewShardedCoordinator(routes, groups, 1, NewHLC(), shards)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	token, err := shards.GetVersionAt(ctx, key, 20)
	require.NoError(t, err)
	require.Equal(t, uint64(10), token.TS)
	require.Equal(t, []byte("source"), token.Value)
	owner, err := shards.ResolveVersionCASGroupForKey(key)
	require.NoError(t, err)
	require.Equal(t, uint64(1), owner)
	staleCAS := &OperationGroup[OP]{IsTxn: true, Elems: []*Elem[OP]{
		{Op: CompareAndSwap, Key: key, ExpectedCommitTS: token.TS, Value: []byte("wrong")},
	}}

	// The target can apply a legacy exact-timestamp overwrite before being
	// enabled. The promoted version has the same numeric identity.
	version := &pb.MVCCVersion{Key: key, CommitTs: token.TS, Value: []byte("copy")}
	importReq := &pb.ImportRangeVersionsRequest{JobId: 1, BracketId: 1, BatchSeq: 1, Versions: []*pb.MVCCVersion{version}}
	require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, targetFSM, importReq, 1))
	importReq.BatchSeq++
	version.Value = []byte("legacy overwrite")
	require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, targetFSM, importReq, 2))
	promote, err := MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 1})
	require.NoError(t, err)
	targetFSM.SetApplyIndex(3)
	require.IsType(t, store.PromoteVersionsResult{}, targetFSM.Apply(promote))
	active, err := MigrationVersionIdentityEnabled(ctx, target)
	require.NoError(t, err)
	require.False(t, active)

	setOwner(2, 2, 20)
	value, err := shards.GetAt(ctx, key, 50)
	require.NoError(t, err)
	require.Equal(t, version.Value, value)
	_, err = shards.GetVersionAt(ctx, key, 50)
	require.ErrorIs(t, err, store.ErrNotSupported)
	_, err = coordinator.Dispatch(ctx, staleCAS)
	require.ErrorIs(t, err, ErrVersionCASUnavailable)
	_, err = shards.ResolveVersionCASGroupForKey(key)
	require.ErrorIs(t, err, ErrVersionCASUnavailable)

	targetFSM.SetApplyIndex(4)
	require.Nil(t, targetFSM.Apply(EncodeMigrationVersionIdentityActivation()))
	freshKey := []byte("fresh")
	require.NoError(t, target.PutAt(ctx, freshKey, []byte("post-cutover"), 30, 0))
	_, err = shards.GetVersionAt(ctx, freshKey, 50)
	require.ErrorIs(t, err, store.ErrNotSupported)
	_, err = coordinator.Dispatch(ctx, staleCAS)
	require.ErrorIs(t, err, ErrVersionCASUnavailable)

	// Cleanup removes the old source row. A return import can then carry
	// the substituted bytes at the same timestamp without an exact row
	// remaining in the source against which to detect the old overwrite.
	cleanup, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId: 1, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart: key, RangeEnd: nextScanCursor(key), MaxCommitTs: 20,
	})
	require.NoError(t, err)
	sourceFSM.SetApplyIndex(2)
	cleaned, ok := sourceFSM.Apply(cleanup).(store.CleanupVersionsResult)
	require.True(t, ok)
	require.Equal(t, uint64(1), cleaned.DeletedRows)
	importReq.JobId, importReq.BatchSeq = 2, 1
	require.IsType(t, store.ImportVersionsResult{}, applyMigrationVersionIdentityImport(t, sourceFSM, importReq, 3))
	promote, err = MarshalMigrationPromoteCommand(&pb.PromoteStagedVersionsRequest{JobId: 2})
	require.NoError(t, err)
	sourceFSM.SetApplyIndex(4)
	require.IsType(t, store.PromoteVersionsResult{}, sourceFSM.Apply(promote))
	setOwner(3, 1, 40)
	returned, err := store.GetVersionAt(ctx, source, key, 50)
	require.NoError(t, err)
	require.Equal(t, token.TS, returned.TS)
	require.Equal(t, version.Value, returned.Value)
	_, err = shards.GetVersionAtWithReadFence(ctx, key, 50, 1, 3)
	require.ErrorIs(t, err, store.ErrNotSupported)
	_, err = coordinator.Dispatch(ctx, staleCAS)
	require.ErrorIs(t, err, ErrVersionCASUnavailable)
	_, err = shards.ResolveVersionCASGroupForKey(key)
	require.ErrorIs(t, err, ErrVersionCASUnavailable)
}

func TestVersionCASApplyRejectsMigrationGuardAfterAdmission(t *testing.T) {
	t.Parallel()
	f, st := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, f)
	ctx := t.Context()
	key := []byte("key")
	require.NoError(t, st.PutAt(ctx, key, []byte("source"), 10, 0))
	request := casApplyRequest(30, 40, 10, string(key), "wrong")
	require.NoError(t, CheckMigrationRequest(ctx, st, 1, request))
	writer, ok := st.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadinessAt(ctx,
		store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true,
			SourceWriteFence: true, RetentionPinTS: 1, RouteStart: key, MinWriteTSExclusive: 20, ExpectedCutoverVersion: 2}, 2))
	require.ErrorIs(t, CheckMigrationRequest(ctx, st, 1, request), ErrVersionCASUnavailable)
	requireCASRejection(t, applyCASFrame(t, f, 3, request), ErrVersionCASUnavailable)
	requireCASProgress(t, f, 3)
	value, err := st.GetAt(ctx, key, 50)
	require.NoError(t, err)
	require.Equal(t, []byte("source"), value)
}

func TestMigrationVersionIdentitySurvivesSnapshot(t *testing.T) {
	t.Parallel()
	src, backing := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, src)
	ctx := t.Context()
	require.NoError(t, backing.PutAt(ctx, []byte("key"), []byte("before"), 10, 0))
	snapshot, err := backing.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	dst, reloaded := newCommitTSFenceFSM(t)
	require.NoError(t, dst.Restore(bytes.NewReader(raw.Bytes())))
	active, err := MigrationVersionIdentityEnabled(ctx, reloaded)
	require.NoError(t, err)
	require.True(t, active)
	require.Nil(t, applyCASFrame(t, dst, 2, casApplyRequest(15, 20, 10, "key", "after")))
	record, err := store.GetVersionAt(ctx, reloaded, []byte("key"), 20)
	require.NoError(t, err)
	require.Equal(t, store.VersionedValue{TS: 20, Value: []byte("after")}, record)
}
