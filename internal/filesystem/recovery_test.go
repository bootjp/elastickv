package filesystem

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestServiceCreateAndDeleteClearDurableIntents(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	_, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	assertIntentCount(t, ctx, svc, 0)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("file")))
	assertIntentCount(t, ctx, svc, 0)
}

func TestServiceRecoverIntentsAbortsPreparedNamespaceMutation(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))

	intent := IntentState{
		ID:        []byte("create-crash"),
		Kind:      IntentKindCreate,
		Phase:     "prepared",
		Parent:    RootInode,
		Name:      []byte("never-committed"),
		CreatedAt: svc.now().UnixNano(),
		UpdatedAt: svc.now().UnixNano(),
	}
	key := fskeys.IntentKey(intent.ID)
	elem, err := putElem(key, intent)
	require.NoError(t, err)
	ts := svc.store.LastCommitTS()
	require.NoError(t, svc.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{key}))
	assertIntentCount(t, ctx, svc, 1)

	stats, err := svc.RecoverIntents(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.IntentsCleared)
	assertIntentCount(t, ctx, svc, 0)
	_, err = svc.Resolve(ctx, RootInode, []byte("never-committed"))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceRecoverIntentsHonorsScanLimit(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))

	for _, id := range []string{"create-a", "create-b", "create-c"} {
		intent := IntentState{
			ID:        []byte(id),
			Kind:      IntentKindCreate,
			Phase:     "prepared",
			Parent:    RootInode,
			Name:      []byte(id),
			CreatedAt: svc.now().UnixNano(),
			UpdatedAt: svc.now().UnixNano(),
		}
		key := fskeys.IntentKey(intent.ID)
		elem, err := putElem(key, intent)
		require.NoError(t, err)
		require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{elem}, [][]byte{key}))
	}

	stats, err := svc.RecoverIntents(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.IntentsCleared)
	assertIntentCount(t, ctx, svc, 1)

	stats, err = svc.RecoverIntents(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.IntentsCleared)
	assertIntentCount(t, ctx, svc, 0)
}

func TestServiceRecoverIntentsPagesPastCompletedMoveJobs(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)
	records := []MoveJob{
		{ID: []byte{1}, Inode: 100, Phase: MovePhaseCompleted},
		{ID: []byte{2}, Inode: 101, Phase: MovePhaseCompleted},
		{
			ID:         []byte{3},
			Inode:      102,
			SourceHome: 10,
			TargetHome: 20,
			Phase:      MovePhaseSourceCleanup,
			Cursor:     fskeys.ChunkPrefix(10, 102),
		},
	}
	for _, job := range records {
		key := fskeys.MoveJobKey(job.ID)
		elem, err := putElem(key, job)
		require.NoError(t, err)
		require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{elem}, [][]byte{key}))
	}

	stats, err := svc.RecoverIntents(ctx, 2)
	require.NoError(t, err)
	require.Zero(t, stats.MoveJobsResumed)
	require.EqualValues(t, 2, stats.MoveJobsCleared)

	stats, err = svc.RecoverIntents(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.MoveJobsResumed)
	require.EqualValues(t, 1, stats.MoveJobsCleared)

	jobs, err := svc.store.ScanAt(
		ctx,
		fskeys.MoveJobAllPrefix(),
		prefixEnd(fskeys.MoveJobAllPrefix()),
		1,
		svc.store.LastCommitTS(),
	)
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestServiceRecoverIntentsToleratesConcurrentJobClear(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := store.NewMVCCStore()
	barrierStore := &concurrentRecoveryScanStore{
		MVCCStore: base,
		release:   make(chan struct{}),
	}
	svc, err := NewService(barrierStore, &testCoordinator{st: barrierStore})
	require.NoError(t, err)
	job := MoveJob{
		ID:         []byte("concurrent"),
		Inode:      102,
		SourceHome: 10,
		TargetHome: 20,
		Phase:      MovePhaseSourceCleanup,
		Cursor:     fskeys.ChunkPrefix(10, 102),
	}
	key := fskeys.MoveJobKey(job.ID)
	elem, err := putElem(key, job)
	require.NoError(t, err)
	require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{elem}, [][]byte{key}))

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, recoverErr := svc.RecoverIntents(ctx, 1)
			errs <- recoverErr
		}()
	}
	for range 2 {
		require.NoError(t, <-errs)
	}
}

type concurrentRecoveryScanStore struct {
	store.MVCCStore
	mu      sync.Mutex
	waiting int
	release chan struct{}
}

func (s *concurrentRecoveryScanStore) ScanAt(
	ctx context.Context,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([]*store.KVPair, error) {
	pairs, err := s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	if err != nil || len(pairs) == 0 || !bytes.Equal(start, fskeys.MoveJobAllPrefix()) {
		return pairs, err
	}
	s.mu.Lock()
	s.waiting++
	if s.waiting == 2 {
		close(s.release)
	}
	release := s.release
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-release:
		return pairs, nil
	}
}

func assertIntentCount(t *testing.T, ctx context.Context, svc *Service, want int) {
	t.Helper()
	prefix := fskeys.IntentAllPrefix()
	page, err := svc.store.ScanAt(ctx, prefix, prefixEnd(prefix), want+1, svc.store.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, page, want)
}
