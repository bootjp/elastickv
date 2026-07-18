package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/stretchr/testify/require"
)

func TestServiceListFilePlacementStatsDetectsStaleCrossGroupChunk(t *testing.T) {
	ctx := context.Background()
	svc, placement := newMigrationTestService(t, &testCoordinatorFactory{}, 2, 100, 101)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode, ClientID: []byte("client")})
	require.NoError(t, err)
	_, err = svc.Write(ctx, created.Inode, created.FH, 0, []byte("data"))
	require.NoError(t, err)
	_, err = svc.MoveFile(ctx, created.Inode, 2)
	require.NoError(t, err)

	staleSource := fskeys.ChunkKey(10, created.Inode, 0)
	require.NoError(t, placement.PutAt(ctx, staleSource, []byte("stale"), placement.LastCommitTS()+1, 0))

	stats, err := svc.ListFilePlacementStats(ctx)
	require.NoError(t, err)
	require.Equal(t, map[uint64]uint64{2: 1}, stats.FilesByGroup)
	require.EqualValues(t, 1, stats.MultiShardFiles)
	require.Equal(t, []uint64{created.Inode}, stats.MultiShardInodes)
	require.Zero(t, stats.MoveInflight)
	require.EqualValues(t, 1, stats.OpenHandleLeases)
	require.Zero(t, stats.OrphanedInodes)
}

func TestServiceListFilePlacementStatsAllowsMigrationDualPlacement(t *testing.T) {
	ctx := context.Background()
	svc, placement := newMigrationTestService(t, &testCoordinatorFactory{}, 2, 100)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	ts := placement.LastCommitTS()
	home, err := svc.homeAt(ctx, created.Inode, ts)
	require.NoError(t, err)
	meta, err := svc.inodeAt(ctx, created.Inode, ts)
	require.NoError(t, err)
	home.State = HomeStateMigrating
	home.TargetHomeSlot = 20
	home.Epoch++
	meta.Epoch = home.Epoch
	elems, err := putElems(fskeys.HomeKey(created.Inode), home, fskeys.InodeKey(created.Inode), meta)
	require.NoError(t, err)
	require.NoError(t, svc.dispatchTxn(ctx, ts, elems, [][]byte{fskeys.HomeKey(created.Inode), fskeys.InodeKey(created.Inode)}))
	require.NoError(t, placement.PutAt(ctx, fskeys.ChunkKey(10, created.Inode, 0), []byte("source"), placement.LastCommitTS()+1, 0))
	require.NoError(t, placement.PutAt(ctx, fskeys.ChunkKey(20, created.Inode, 0), []byte("target"), placement.LastCommitTS()+1, 0))

	stats, err := svc.ListFilePlacementStats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.MultiShardFiles)
	require.Empty(t, stats.MultiShardInodes)
}
