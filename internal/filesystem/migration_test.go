package filesystem

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestServiceMoveFileCopiesSwitchesAndCleansSource(t *testing.T) {
	ctx := context.Background()
	svc, placement := newMigrationTestService(t, &testCoordinatorFactory{}, 2, 100, 101)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	payload := bytes.Repeat([]byte("abcd"), movePageSize+2)
	_, err = svc.Write(ctx, created.Inode, 0, 0, payload)
	require.NoError(t, err)
	usageBefore, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)

	job, err := svc.MoveFile(ctx, created.Inode, 2)
	require.NoError(t, err)
	require.Equal(t, MovePhaseCompleted, job.Phase)
	require.EqualValues(t, 10, job.SourceHome)
	require.EqualValues(t, 20, job.TargetHome)
	require.Greater(t, job.CopiedChunks, uint64(movePageSize))
	require.Equal(t, job.CopiedChunks, job.CleanedChunks)

	home, err := svc.GetFileHome(ctx, created.Inode)
	require.NoError(t, err)
	require.Equal(t, HomeStateActive, home.State)
	require.EqualValues(t, 20, home.HomeSlot)
	require.Zero(t, home.TargetHomeSlot)
	require.EqualValues(t, 3, home.Epoch)

	got, err := svc.Read(ctx, created.Inode, 0, 0, uint64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got)
	assertChunkPrefixEmpty(t, ctx, placement, fskeys.ChunkPrefix(10, created.Inode))
	require.LessOrEqual(t, job.CopiedChunks, uint64(math.MaxInt))
	assertChunkPrefixCount(t, ctx, placement, fskeys.ChunkPrefix(20, created.Inode), int(job.CopiedChunks)) //nolint:gosec // bounded by math.MaxInt above.

	usageAfter, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, usageBefore.Files, usageAfter.Files)
	require.Equal(t, usageBefore.Capacity-usageBefore.Free, usageAfter.Capacity-usageAfter.Free)
}

func TestServiceResumeMoveFileAfterSwitchFailure(t *testing.T) {
	ctx := context.Background()
	base := store.NewMVCCStore()
	placement := &migrationPlacementStore{MVCCStore: base, targetHomes: map[uint64]uint64{1: 10, 2: 20}}
	normal := &testCoordinator{st: placement}
	failing := &failNthDispatchCoordinator{inner: normal, failAt: 9, err: context.DeadlineExceeded}
	svc, err := NewService(
		placement,
		failing,
		WithChunkSize(testChunkSize),
		WithIDAllocator(sequenceIDAllocator(2, 100, 101)),
		WithHomeSlotAllocator(func(uint64) uint64 { return 10 }),
	)
	require.NoError(t, err)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, created.Inode, 0, 0, []byte("payload"))
	require.NoError(t, err)

	job, err := svc.MoveFile(ctx, created.Inode, 2)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, job.ID)

	jobs, err := placement.ScanAt(ctx, fskeys.MoveJobAllPrefix(), prefixEnd(fskeys.MoveJobAllPrefix()), 10, placement.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	persisted, err := decodeJSON[MoveJob](jobs[0].Value)
	require.NoError(t, err)
	require.Equal(t, MovePhaseSwitch, persisted.Phase)

	recovered, err := NewService(
		placement,
		normal,
		WithChunkSize(testChunkSize),
		WithIDAllocator(sequenceIDAllocator(200)),
		WithHomeSlotAllocator(func(uint64) uint64 { return 10 }),
	)
	require.NoError(t, err)
	stats, err := recovered.RecoverIntents(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.MoveJobsResumed)
	completed, err := recovered.moveJob(ctx, persisted.ID)
	require.NoError(t, err)
	require.Equal(t, MovePhaseCompleted, completed.Phase)
	got, err := recovered.Read(ctx, created.Inode, 0, 0, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}

func TestServiceMoveFileRetriesConcurrentRecoveryConflict(t *testing.T) {
	ctx := context.Background()
	svc, placement := newMigrationTestService(t, &testCoordinatorFactory{}, 2, 100)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, created.Inode, 0, 0, []byte("payload"))
	require.NoError(t, err)

	conflicting := &failNthDispatchCoordinator{
		inner:  &testCoordinator{st: placement},
		failAt: 2,
		err:    store.ErrWriteConflict,
	}
	svc.dispatch = conflicting
	job, err := svc.MoveFile(ctx, created.Inode, 2)
	require.NoError(t, err)
	require.Equal(t, MovePhaseCompleted, job.Phase)
	require.Greater(t, conflicting.calls, 2)
}

func TestServiceRejectsDataMutationWhileHomeIsMigrating(t *testing.T) {
	ctx := context.Background()
	svc, _ := newMigrationTestService(t, &testCoordinatorFactory{}, 2, 100)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	ts := svc.store.LastCommitTS()
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

	_, err = svc.Write(ctx, created.Inode, 0, 0, []byte("x"))
	require.ErrorIs(t, err, ErrStaleHome)
	require.ErrorIs(t, svc.Truncate(ctx, created.Inode, 0), ErrStaleHome)
	_, err = svc.SetAttr(ctx, created.Inode, SetAttrMask{Size: true}, SetAttr{Size: 0})
	require.ErrorIs(t, err, ErrStaleHome)
	require.ErrorIs(t, svc.Unlink(ctx, RootInode, []byte("file")), ErrStaleHome)
}

type testCoordinatorFactory struct{}

func (*testCoordinatorFactory) coordinator(st store.MVCCStore) Dispatcher {
	return &testCoordinator{st: st}
}

type migrationPlacementStore struct {
	store.MVCCStore
	targetHomes map[uint64]uint64
}

func (s *migrationPlacementStore) FilesystemGroupForHome(homeSlot uint64, _ uint64) (uint64, bool) {
	for groupID, home := range s.targetHomes {
		if home == homeSlot {
			return groupID, true
		}
	}
	return 0, false
}

func (s *migrationPlacementStore) ResolveFilesystemHomeSlot(targetGroup uint64, _ uint64) (uint64, error) {
	home, ok := s.targetHomes[targetGroup]
	if !ok {
		return 0, ErrInvalid
	}
	return home, nil
}

func newMigrationTestService(
	t *testing.T,
	factory *testCoordinatorFactory,
	ids ...uint64,
) (*Service, *migrationPlacementStore) {
	t.Helper()
	placement := &migrationPlacementStore{
		MVCCStore:   store.NewMVCCStore(),
		targetHomes: map[uint64]uint64{1: 10, 2: 20},
	}
	svc, err := NewService(
		placement,
		factory.coordinator(placement),
		WithChunkSize(testChunkSize),
		WithIDAllocator(sequenceIDAllocator(ids...)),
		WithHomeSlotAllocator(func(uint64) uint64 { return 10 }),
	)
	require.NoError(t, err)
	return svc, placement
}

func assertChunkPrefixEmpty(t *testing.T, ctx context.Context, st store.MVCCStore, prefix []byte) {
	t.Helper()
	assertChunkPrefixCount(t, ctx, st, prefix, 0)
}

func assertChunkPrefixCount(t *testing.T, ctx context.Context, st store.MVCCStore, prefix []byte, count int) {
	t.Helper()
	page, err := st.ScanAt(ctx, prefix, prefixEnd(prefix), count+1, st.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, page, count)
}
