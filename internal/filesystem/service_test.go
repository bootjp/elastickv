package filesystem

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testRootMode  uint32 = 0o755
	testFileMode  uint32 = 0o644
	testDirMode   uint32 = 0o755
	testChunkSize uint64 = 4
)

// testCoordinator applies groups to st through kvtest.LocalCoordinator with
// fstest.MemoryDispatcher's policy: commit at max(store+1, requested,
// StartTS+1), OCC on every group, commit-timestamp patching, and no prefix
// deletes.
type testCoordinator struct {
	st    store.MVCCStore
	once  sync.Once
	local *kvtest.LocalCoordinator
}

func (c *testCoordinator) coordinator() *kvtest.LocalCoordinator {
	c.once.Do(func() {
		c.local = kvtest.NewLocalCoordinator(c.st, kvtest.LocalOptions{
			DelPrefix:        kvtest.PrefixReject,
			CommitTimestamp:  kvtest.TimestampStoreIncrement,
			AdvanceCommitTS:  true,
			OCC:              kvtest.OCCAllGroups,
			ValidateReadKeys: true,
			PatchCommitTS:    true,
		})
	})
	return c.local
}

type rootRaceCoordinator struct {
	inner     *testCoordinator
	once      sync.Once
	winnerErr error
	ambiguous bool
}

func (c *rootRaceCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *rootRaceCoordinator) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	c.once.Do(func() {
		_, c.winnerErr = c.inner.Dispatch(ctx, req)
	})
	if c.winnerErr != nil {
		return nil, c.winnerErr
	}
	resp, err := c.inner.Dispatch(ctx, req)
	if err != nil && c.ambiguous {
		return nil, fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, status.Error(codes.Unknown, err.Error()))
	}
	return resp, err
}

func (c *testCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.coordinator().LinearizableRead(ctx)
}

func (c *testCoordinator) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	return c.coordinator().Dispatch(ctx, req)
}

type recordingCoordinator struct {
	inner    *testCoordinator
	requests []*kv.OperationGroup[kv.OP]
}

func (c *recordingCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *recordingCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	copied := *req
	copied.Elems = append([]*kv.Elem[kv.OP](nil), req.Elems...)
	copied.ReadKeys = cloneKeys(req.ReadKeys)
	c.requests = append(c.requests, &copied)
	return c.inner.Dispatch(ctx, req)
}

type staleLinearizableCoordinator struct {
	inner *testCoordinator
	ts    uint64
	calls int
}

func (c *staleLinearizableCoordinator) LinearizableRead(context.Context) (uint64, error) {
	c.calls++
	return c.ts, nil
}

func (c *staleLinearizableCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return c.inner.Dispatch(ctx, req)
}

type allGroupsFenceCoordinator struct {
	inner             *testCoordinator
	allGroupsCalls    int
	linearizableCalls int
	fenceErr          error
}

func (c *allGroupsFenceCoordinator) LeaseReadAllGroups(context.Context) error {
	c.allGroupsCalls++
	return c.fenceErr
}

func (c *allGroupsFenceCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	c.linearizableCalls++
	return c.inner.LinearizableRead(ctx)
}

func (c *allGroupsFenceCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return c.inner.Dispatch(ctx, req)
}

type writeConflictCoordinator struct {
	inner *testCoordinator
	calls int
}

func (c *writeConflictCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *writeConflictCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.calls++
	return nil, store.ErrWriteConflict
}

type transientWriteConflictCoordinator struct {
	inner     *testCoordinator
	remaining int
	calls     int
	failure   error
}

func TestServiceCreateReplansAfterDefinitiveConflict(t *testing.T) {
	ctx := t.Context()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{inner: inner, remaining: 1}
	svc.dispatch = conflicts
	created, err := svc.Create(ctx, RootInode, []byte("retry-create"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.NotZero(t, created.Inode)
	require.GreaterOrEqual(t, conflicts.calls, 2)
	resolved, err := svc.Resolve(ctx, RootInode, []byte("retry-create"))
	require.NoError(t, err)
	require.Equal(t, created.Inode, resolved)
}

func TestServiceCreateDoesNotReplayUnknownOutcome(t *testing.T) {
	ctx := t.Context()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{
		inner: inner, remaining: 1,
		failure: fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict),
	}
	svc.dispatch = conflicts
	_, err := svc.Create(ctx, RootInode, []byte("unknown-create"), CreateOptions{Mode: testFileMode})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, conflicts.calls)
}

func TestServiceNamespaceMutationsReplanAfterDefinitiveConflict(t *testing.T) {
	for _, operation := range []string{"unlink", "rename"} {
		t.Run(operation, func(t *testing.T) {
			ctx := t.Context()
			svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))
			require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
			created, err := svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
			require.NoError(t, err)
			inner, ok := svc.dispatch.(*testCoordinator)
			require.True(t, ok)
			conflicts := &transientWriteConflictCoordinator{inner: inner, remaining: 1}
			svc.dispatch = conflicts
			if operation == "unlink" {
				err = svc.Unlink(ctx, RootInode, []byte("source"))
			} else {
				err = svc.Rename(ctx, RootInode, []byte("source"), RootInode, []byte("target"))
			}
			require.NoError(t, err)
			require.GreaterOrEqual(t, conflicts.calls, 2)
			_, err = svc.Resolve(ctx, RootInode, []byte("source"))
			require.ErrorIs(t, err, ErrNotFound)
			if operation == "rename" {
				resolved, err := svc.Resolve(ctx, RootInode, []byte("target"))
				require.NoError(t, err)
				require.Equal(t, created.Inode, resolved)
			}
		})
	}
}

func TestServiceReleaseReplansAfterDefinitiveConflict(t *testing.T) {
	ctx := t.Context()
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4}, WithCapacity(16))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	clientID := []byte("release-client")
	created, err := svc.Create(ctx, RootInode, []byte("release"), CreateOptions{
		Mode: testFileMode, ClientID: clientID,
	})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{inner: inner, remaining: 1}
	svc.dispatch = conflicts
	require.NoError(t, svc.Release(ctx, created.Inode, created.FH, clientID))
	require.GreaterOrEqual(t, conflicts.calls, 2)
	_, err = svc.store.GetAt(ctx, fskeys.RefKey(created.Inode, clientID, created.FH), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

type interleavingWriteConflictCoordinator struct {
	inner      *testCoordinator
	compete    func(context.Context) error
	interfered bool
}

type lostWriteResponseCoordinator struct {
	inner *testCoordinator
	after func(context.Context) error
	calls int
}

func (c *lostWriteResponseCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *lostWriteResponseCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.calls++
	if _, err := c.inner.Dispatch(ctx, req); err != nil {
		return nil, err
	}
	if err := c.after(ctx); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)
}

func (c *interleavingWriteConflictCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *interleavingWriteConflictCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if !c.interfered {
		c.interfered = true
		if err := c.compete(ctx); err != nil {
			return nil, err
		}
		return nil, store.ErrWriteConflict
	}
	return c.inner.Dispatch(ctx, req)
}

func (c *transientWriteConflictCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *transientWriteConflictCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.calls++
	if c.remaining > 0 {
		c.remaining--
		if c.failure != nil {
			return nil, c.failure
		}
		return nil, store.ErrWriteConflict
	}
	return c.inner.Dispatch(ctx, req)
}

type failNthDispatchCoordinator struct {
	inner  *testCoordinator
	failAt int
	calls  int
	err    error
}

type afterSuccessfulDispatchCoordinator struct {
	inner  *testCoordinator
	after  func(context.Context)
	called bool
}

func (c *afterSuccessfulDispatchCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *afterSuccessfulDispatchCoordinator) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	resp, err := c.inner.Dispatch(ctx, req)
	if err == nil && !c.called {
		c.called = true
		c.after(ctx)
	}
	return resp, err
}

func (c *failNthDispatchCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *failNthDispatchCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.calls++
	if c.calls == c.failAt {
		return nil, c.err
	}
	return c.inner.Dispatch(ctx, req)
}

func TestServiceWriteRebuildsAfterDefinitiveConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{inner: inner, remaining: 2}
	svc.dispatch = conflicts

	n, err := svc.Write(ctx, created.Inode, created.FH, 0, []byte("abc"))
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, 3, conflicts.calls)
	read, err := svc.Read(ctx, created.Inode, created.FH, 0, 3)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), read)
}

func TestServiceWriteConflictRetainsConcurrentDisjointBytes(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, created.Inode, created.FH, 0, []byte("00"))
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	other, err := NewService(svc.store, inner, WithChunkSize(testChunkSize))
	require.NoError(t, err)
	conflicts := &interleavingWriteConflictCoordinator{inner: inner}
	conflicts.compete = func(ctx context.Context) error {
		_, err := other.Write(ctx, created.Inode, created.FH, 1, []byte("B"))
		return err
	}
	svc.dispatch = conflicts

	_, err = svc.Write(ctx, created.Inode, created.FH, 0, []byte("A"))
	require.NoError(t, err)
	require.True(t, conflicts.interfered)
	read, err := svc.Read(ctx, created.Inode, created.FH, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []byte("AB"), read)
}

func TestServiceWriteNeverReplaysUnknownOutcomeWithConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{
		inner: inner, remaining: 1,
		failure: fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict),
	}
	svc.dispatch = conflicts

	_, err = svc.Write(ctx, created.Inode, created.FH, 0, []byte("abc"))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, conflicts.calls)
}

func TestServiceWriteLostReplyDoesNotOverwriteLaterWriter(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	other, err := NewService(svc.store, inner, WithChunkSize(testChunkSize))
	require.NoError(t, err)
	lost := &lostWriteResponseCoordinator{inner: inner}
	lost.after = func(ctx context.Context) error {
		_, err := other.Write(ctx, created.Inode, created.FH, 0, []byte("B"))
		return err
	}
	svc.dispatch = lost

	_, err = svc.Write(ctx, created.Inode, created.FH, 0, []byte("A"))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, lost.calls)
	read, err := svc.Read(ctx, created.Inode, created.FH, 0, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("B"), read)
}

func TestServiceOpenDoesNotRetryUnknownOutcomeWithConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{
		inner: inner, remaining: 1,
		failure: fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict),
	}
	svc.dispatch = conflicts

	_, err = svc.Open(ctx, created.Inode, []byte("client-a"))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, conflicts.calls)
}

func TestServiceOpenRebuildsAfterDefinitiveConflicts(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{inner: inner, remaining: 5}
	svc.dispatch = conflicts

	fh, err := svc.Open(ctx, created.Inode, []byte("client-a"))
	require.NoError(t, err)
	require.NotZero(t, fh)
	require.Equal(t, 6, conflicts.calls)
}

func TestServiceOpenWaitsForPendingTransactionLock(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	locked := &transientWriteConflictCoordinator{inner: inner, remaining: 1, failure: kv.ErrTxnLocked}
	svc.dispatch = locked

	fh, err := svc.Open(ctx, created.Inode, []byte("client-a"))
	require.NoError(t, err)
	require.NotZero(t, fh)
	require.Equal(t, 2, locked.calls)
}

func TestServiceRefreshLeaseRebuildsAfterDefinitiveConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode, ClientID: []byte("client-a")})
	require.NoError(t, err)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{inner: inner, remaining: 2}
	svc.dispatch = conflicts

	require.NoError(t, svc.RefreshOpenHandleLease(ctx, created.Inode, created.FH, []byte("client-a")))
	require.Equal(t, 3, conflicts.calls)
}

func TestServiceCreateWriteReadTruncateSparse(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{
		Mode:     testFileMode,
		UID:      1000,
		GID:      1000,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, created.Inode)
	require.EqualValues(t, 3, created.FH)

	n, err := svc.Write(ctx, created.Inode, created.FH, 2, []byte("abcde"))
	require.NoError(t, err)
	require.Equal(t, len("abcde"), n)

	got, err := svc.Read(ctx, created.Inode, created.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 'a', 'b', 'c', 'd', 'e'}, got)

	require.NoError(t, svc.Truncate(ctx, created.Inode, 5))
	got, err = svc.Read(ctx, created.Inode, created.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 'a', 'b', 'c'}, got)

	require.NoError(t, svc.Truncate(ctx, created.Inode, 9))
	got, err = svc.Read(ctx, created.Inode, created.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 'a', 'b', 'c', 0, 0, 0, 0}, got)
}

func TestServiceInitializeRootToleratesConcurrentWinner(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	inner := &testCoordinator{st: st}
	coord := &rootRaceCoordinator{inner: inner}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize))
	require.NoError(t, err)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	require.NoError(t, coord.winnerErr)
	root, err := svc.GetAttr(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, TypeDirectory, root.Type)
	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Files)
}

func TestServiceInitializeRootResolvesUnknownForwardedConflict(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := &rootRaceCoordinator{inner: &testCoordinator{st: st}, ambiguous: true}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize))
	require.NoError(t, err)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	root, err := svc.GetAttr(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, TypeDirectory, root.Type)
	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.Files)
}

func TestServiceWriteReadAtSaturatedFinalChunkBoundary(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	created, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.SetAttr(ctx, created.Inode, SetAttrMask{Size: true}, SetAttr{Size: math.MaxUint64})
	require.NoError(t, err)

	offset := math.MaxUint64 - testChunkSize + 1
	want := []byte("xyz")
	n, err := svc.Write(ctx, created.Inode, 0, offset, want)
	require.NoError(t, err)
	require.Equal(t, len(want), n)

	got, err := svc.Read(ctx, created.Inode, 0, offset, uint64(len(want)))
	require.NoError(t, err)
	require.Equal(t, want, got)

	got, err = svc.Read(ctx, created.Inode, 0, offset+1, uint64(len(want))+1)
	require.NoError(t, err)
	require.Equal(t, want[1:], got)
}

func TestServiceReadTSUsesMVCCTimestampAfterLinearizableFence(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	inner := &testCoordinator{st: st}
	coord := &staleLinearizableCoordinator{inner: inner}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize), WithIDAllocator(sequenceIDAllocator(2)))
	require.NoError(t, err)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	stat, err := svc.GetAttr(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, RootInode, stat.Inode)
	require.Positive(t, coord.calls)
}

func TestServiceReadTSUsesAllGroupsFenceBeforeSnapshot(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("key"), []byte("value"), 7, 0))
	inner := &testCoordinator{st: st}
	coord := &allGroupsFenceCoordinator{inner: inner}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize), WithIDAllocator(sequenceIDAllocator(2)))
	require.NoError(t, err)

	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 7, ts)
	require.Equal(t, 1, coord.allGroupsCalls)
	require.Zero(t, coord.linearizableCalls)
}

func TestServiceFullChunkWriteSkipsChunkReadKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	rec := attachRecorder(svc)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcd"))
	require.NoError(t, err)
	require.Len(t, rec.requests, 1)
	require.False(t, keyInSet(rec.requests[0].ReadKeys, fskeys.ChunkKey(file.Inode, file.Inode, 0)),
		"full chunk overwrite should not add the chunk to readKeys")

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 4, stats.Capacity-stats.Free)
}

func TestServiceFullChunkWriteChargesSparseChunkBytes(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.NoError(t, svc.Truncate(ctx, file.Inode, testChunkSize))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)

	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcd"))
	require.NoError(t, err)
	stats, err = svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 4, stats.Capacity-stats.Free)

	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte{0, 0, 0, 0})
	require.NoError(t, err)
	stats, err = svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(file.Inode, file.Inode, 0), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceSetAttrSizeAndModeUsesSingleTxn(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcdef"))
	require.NoError(t, err)

	rec := attachRecorder(svc)
	stat, err := svc.SetAttr(ctx, file.Inode, SetAttrMask{Size: true, Mode: true}, SetAttr{
		Size: 3,
		Mode: 0o600,
	})
	require.NoError(t, err)
	require.Len(t, rec.requests, 1)
	require.EqualValues(t, 3, stat.Size)
	require.EqualValues(t, 0o600, stat.Mode)
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.ChunkKey(file.Inode, file.Inode, 0)))
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.InodeKey(file.Inode)))
}

func TestServiceSetAttrSizeOnlyUpdatesMtime(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	svc := newTestServiceWithOptions(t, []uint64{2}, WithClock(func() time.Time { return now }))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcdef"))
	require.NoError(t, err)
	before, err := svc.GetAttr(ctx, file.Inode)
	require.NoError(t, err)

	now = now.Add(time.Second)
	stat, err := svc.SetAttr(ctx, file.Inode, SetAttrMask{Size: true}, SetAttr{Size: 3})
	require.NoError(t, err)
	require.EqualValues(t, 3, stat.Size)
	require.Equal(t, now.UnixNano(), stat.MtimeNsec)
	require.Greater(t, stat.MtimeNsec, before.MtimeNsec)
}

func TestServiceOpenUsesRefFenceWithoutTouchingInode(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	rec := attachRecorder(svc)
	fh, err := svc.Open(ctx, file.Inode, []byte("client-a"))
	require.NoError(t, err)
	require.EqualValues(t, 3, fh)
	require.Len(t, rec.requests, 1)
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefKey(file.Inode, []byte("client-a"), fh)))
	require.Equal(t, fskeys.RefKey(file.Inode, []byte("client-a"), fh), rec.requests[0].PrimaryKey)
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefFenceKey(file.Inode)))
	require.False(t, elemTouchesKey(rec.requests[0].Elems, fskeys.InodeKey(file.Inode)))
}

func TestServiceOpenRetriesDuplicateHandleID(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 3, 4)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	require.EqualValues(t, 3, file.FH)

	fh, err := svc.Open(ctx, file.Inode, []byte("client-a"))
	require.NoError(t, err)
	require.EqualValues(t, 4, fh)

	_, err = svc.store.GetAt(ctx, fskeys.RefKey(file.Inode, []byte("client-a"), file.FH), svc.store.LastCommitTS())
	require.NoError(t, err)
	_, err = svc.store.GetAt(ctx, fskeys.RefKey(file.Inode, []byte("client-a"), fh), svc.store.LastCommitTS())
	require.NoError(t, err)
}

func TestServiceReaddirRenameAndRmdir(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	dir, err := svc.Mkdir(ctx, RootInode, []byte("dir"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	file, err := svc.Create(ctx, RootInode, []byte("a"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	first, err := svc.Readdir(ctx, RootInode, "", 2)
	require.NoError(t, err)
	require.Len(t, first.Entries, 2)
	require.Equal(t, []byte("."), first.Entries[0].Name)
	require.Equal(t, []byte(".."), first.Entries[1].Name)
	require.NotEmpty(t, first.NextCookie)

	second, err := svc.Readdir(ctx, RootInode, first.NextCookie, 8)
	require.NoError(t, err)
	require.Len(t, second.Entries, 2)
	require.Equal(t, []byte("a"), second.Entries[0].Name)
	require.Equal(t, file.Inode, second.Entries[0].Inode)
	require.Equal(t, []byte("dir"), second.Entries[1].Name)
	require.Equal(t, dir.Inode, second.Entries[1].Inode)

	require.NoError(t, svc.Rename(ctx, RootInode, []byte("a"), RootInode, []byte("b")))
	_, err = svc.Resolve(ctx, RootInode, []byte("a"))
	require.ErrorIs(t, err, ErrNotFound)
	got, err := svc.Resolve(ctx, RootInode, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, file.Inode, got)

	require.NoError(t, svc.Rmdir(ctx, RootInode, []byte("dir")))
	entries, err := svc.Readdir(ctx, RootInode, "", 8)
	require.NoError(t, err)
	require.False(t, containsDirent(entries.Entries, []byte("dir")))
}

func TestServiceReaddirRefreshesCompactedCookieSnapshot(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	_, err := svc.Create(ctx, RootInode, []byte("a"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Create(ctx, RootInode, []byte("b"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	first, err := svc.Readdir(ctx, RootInode, "", 3)
	require.NoError(t, err)
	require.NotEmpty(t, first.NextCookie)
	require.True(t, containsDirent(first.Entries, []byte("a")))

	_, err = svc.Create(ctx, RootInode, []byte("c"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.NoError(t, svc.store.Compact(ctx, svc.store.LastCommitTS()))

	second, err := svc.Readdir(ctx, RootInode, first.NextCookie, 8)
	require.NoError(t, err)
	require.False(t, containsDirent(second.Entries, []byte("a")))
	require.True(t, containsDirent(second.Entries, []byte("b")))
	require.True(t, containsDirent(second.Entries, []byte("c")))
}

func TestServiceRenameSameNameStillChecksSource(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	err := svc.Rename(ctx, RootInode, []byte("missing"), RootInode, []byte("missing"))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceRenameOverExistingFileGcsTarget(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3}, WithCapacity(16), WithMaxFiles(10))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	src, err := svc.Create(ctx, RootInode, []byte("a"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	dst, err := svc.Create(ctx, RootInode, []byte("b"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, dst.Inode, 0, 0, []byte("payload"))
	require.NoError(t, err)

	require.NoError(t, svc.Rename(ctx, RootInode, []byte("a"), RootInode, []byte("b")))
	got, err := svc.Resolve(ctx, RootInode, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, src.Inode, got)
	_, err = svc.GetAttr(ctx, dst.Inode)
	require.ErrorIs(t, err, ErrNotFound)

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.Files)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)
}

func TestServiceRenameOverOpenFileOrphansTarget(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	src, err := svc.Create(ctx, RootInode, []byte("a"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	dst, err := svc.Create(ctx, RootInode, []byte("b"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, dst.Inode, dst.FH, 0, []byte("payload"))
	require.NoError(t, err)

	require.NoError(t, svc.Rename(ctx, RootInode, []byte("a"), RootInode, []byte("b")))
	got, err := svc.Resolve(ctx, RootInode, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, src.Inode, got)
	data, err := svc.Read(ctx, dst.Inode, dst.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), data)

	require.NoError(t, svc.Release(ctx, dst.Inode, dst.FH, []byte("client-a")))
	_, err = svc.GetAttr(ctx, dst.Inode)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceRenameDirectoryOverFileIsRejected(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	dir, err := svc.Mkdir(ctx, RootInode, []byte("dir"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	err = svc.Rename(ctx, RootInode, []byte("dir"), RootInode, []byte("file"))
	require.ErrorIs(t, err, ErrNotDir)

	gotDir, err := svc.Resolve(ctx, RootInode, []byte("dir"))
	require.NoError(t, err)
	require.Equal(t, dir.Inode, gotDir)
	gotFile, err := svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	require.Equal(t, file.Inode, gotFile)
}

func TestServiceRenameDirectoryOverEmptyDirectoryRemovesTarget(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3}, WithMaxFiles(10))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	src, err := svc.Mkdir(ctx, RootInode, []byte("src"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	dst, err := svc.Mkdir(ctx, RootInode, []byte("dst"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)

	require.NoError(t, svc.Rename(ctx, RootInode, []byte("src"), RootInode, []byte("dst")))
	_, err = svc.Resolve(ctx, RootInode, []byte("src"))
	require.ErrorIs(t, err, ErrNotFound)
	got, err := svc.Resolve(ctx, RootInode, []byte("dst"))
	require.NoError(t, err)
	require.Equal(t, src.Inode, got)
	_, err = svc.GetAttr(ctx, dst.Inode)
	require.ErrorIs(t, err, ErrNotFound)
	root, err := svc.GetAttr(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, directoryInitialNlink+1, root.Nlink)
	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.Files)
}

func TestServiceRenameDirectoryOverNonEmptyDirectoryIsRejected(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	src, err := svc.Mkdir(ctx, RootInode, []byte("src"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	dst, err := svc.Mkdir(ctx, RootInode, []byte("dst"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	_, err = svc.Create(ctx, dst.Inode, []byte("child"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	err = svc.Rename(ctx, RootInode, []byte("src"), RootInode, []byte("dst"))
	require.ErrorIs(t, err, ErrNotEmpty)
	gotSrc, err := svc.Resolve(ctx, RootInode, []byte("src"))
	require.NoError(t, err)
	require.Equal(t, src.Inode, gotSrc)
	gotDst, err := svc.Resolve(ctx, RootInode, []byte("dst"))
	require.NoError(t, err)
	require.Equal(t, dst.Inode, gotDst)
}

func TestServiceRmdirRejectsNonEmptyDirectory(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	dir, err := svc.Mkdir(ctx, RootInode, []byte("dir"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	_, err = svc.Create(ctx, dir.Inode, []byte("child"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	err = svc.Rmdir(ctx, RootInode, []byte("dir"))
	require.ErrorIs(t, err, ErrNotEmpty)
}

func TestServiceUnlinkOpenFileKeepsInodeReadable(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)

	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))
	_, err = svc.Resolve(ctx, RootInode, []byte("open"))
	require.ErrorIs(t, err, ErrNotFound)
	got, err := svc.Read(ctx, file.Inode, file.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}

func TestServiceOpenRejectsOrphanedInode(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)

	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))
	_, err = svc.Open(ctx, file.Inode, []byte("client-b"))
	require.ErrorIs(t, err, ErrNotFound)

	got, err := svc.Read(ctx, file.Inode, file.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}

func TestServiceUnlinkFencesObservedOpenRef(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)

	rec := attachRecorder(svc)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))
	require.Len(t, rec.requests, 2)
	require.True(t, keyInSet(rec.requests[1].ReadKeys, fskeys.RefKey(file.Inode, []byte("client-a"), file.FH)))
}

func TestServiceReleaseLastOpenHandleGcsOrphan(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)

	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))
	require.NoError(t, svc.Release(ctx, file.Inode, file.FH, []byte("client-a")))

	_, err = svc.GetAttr(ctx, file.Inode)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(file.Inode, file.Inode, 0), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceReleaseLastOpenHandleGcsInSingleTxn(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))

	rec := attachRecorder(svc)
	require.NoError(t, svc.Release(ctx, file.Inode, file.FH, []byte("client-a")))
	require.Len(t, rec.requests, 1)
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefKey(file.Inode, []byte("client-a"), file.FH)))
	require.Equal(t, fskeys.RefKey(file.Inode, []byte("client-a"), file.FH), rec.requests[0].PrimaryKey)
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.InodeKey(file.Inode)))
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefFenceKey(file.Inode)))
	requireUsageRouteCounterWithTxn(t, rec.requests[0].Elems)
	require.False(t, elemTouchesKey(rec.requests[0].Elems, fskeys.UsageKey()))

	_, err = svc.GetAttr(ctx, file.Inode)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceReapsExpiredOpenHandleAndGcsOrphan(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	svc := newTestServiceWithOptions(t, []uint64{2, 3},
		WithClock(func() time.Time { return now }),
		WithOpenHandleLeaseTTL(time.Second),
	)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))

	now = now.Add(2 * time.Second)
	stats, err := svc.ReapExpiredOpenHandleLeases(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ExpiredRefs)
	require.EqualValues(t, 1, stats.OrphanedInodesGCed)
	_, err = svc.GetAttr(ctx, file.Inode)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceReapExpiredOpenHandleGcsInSingleTxn(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	svc := newTestServiceWithOptions(t, []uint64{2, 3},
		WithClock(func() time.Time { return now }),
		WithOpenHandleLeaseTTL(time.Second),
	)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))

	now = now.Add(2 * time.Second)
	rec := attachRecorder(svc)
	stats, err := svc.ReapExpiredOpenHandleLeases(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ExpiredRefs)
	require.EqualValues(t, 1, stats.OrphanedInodesGCed)
	require.Len(t, rec.requests, 1)
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefKey(file.Inode, []byte("client-a"), file.FH)))
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.InodeKey(file.Inode)))
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefFenceKey(file.Inode)))
	requireUsageRouteCounterWithTxn(t, rec.requests[0].Elems)
	require.False(t, elemTouchesKey(rec.requests[0].Elems, fskeys.UsageKey()))
}

func TestServiceRefreshOpenHandleLeasePreventsExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	svc := newTestServiceWithOptions(t, []uint64{2, 3},
		WithClock(func() time.Time { return now }),
		WithOpenHandleLeaseTTL(time.Second),
	)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))

	now = now.Add(900 * time.Millisecond)
	require.NoError(t, svc.RefreshOpenHandleLease(ctx, file.Inode, file.FH, []byte("client-a")))
	now = now.Add(500 * time.Millisecond)
	stats, err := svc.ReapExpiredOpenHandleLeases(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, stats.ExpiredRefs)
	got, err := svc.Read(ctx, file.Inode, file.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)

	now = now.Add(2 * time.Second)
	stats, err = svc.ReapExpiredOpenHandleLeases(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ExpiredRefs)
	require.EqualValues(t, 1, stats.OrphanedInodesGCed)
	_, err = svc.GetAttr(ctx, file.Inode)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestServiceReapExpiredOpenHandleRevalidatesCurrentLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	svc := newTestServiceWithOptions(t, []uint64{2, 3},
		WithClock(func() time.Time { return now }),
		WithOpenHandleLeaseTTL(time.Second),
	)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("open"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("open")))

	now = now.Add(2 * time.Second)
	refKey := fskeys.RefKey(file.Inode, []byte("client-a"), file.FH)
	staleRaw, err := svc.store.GetAt(ctx, refKey, svc.store.LastCommitTS())
	require.NoError(t, err)
	require.NoError(t, svc.RefreshOpenHandleLease(ctx, file.Inode, file.FH, []byte("client-a")))

	reaped, gcd, err := svc.reapExpiredOpenHandleLease(ctx, &store.KVPair{
		Key:   refKey,
		Value: staleRaw,
	}, now.UnixNano())
	require.NoError(t, err)
	require.False(t, reaped)
	require.False(t, gcd)
	got, err := svc.Read(ctx, file.Inode, file.FH, 0, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got)
}

func TestServiceReapExpiredOpenHandlePagesPastLiveRefs(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4, 5},
		WithClock(func() time.Time { return now }),
		WithOpenHandleLeaseTTL(time.Second),
	)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	live, err := svc.Create(ctx, RootInode, []byte("live"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	expired, err := svc.Create(ctx, RootInode, []byte("expired"), CreateOptions{
		Mode:     testFileMode,
		ClientID: []byte("client-a"),
	})
	require.NoError(t, err)
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("live")))
	require.NoError(t, svc.Unlink(ctx, RootInode, []byte("expired")))

	now = now.Add(2 * time.Second)
	require.NoError(t, svc.RefreshOpenHandleLease(ctx, live.Inode, live.FH, []byte("client-a")))
	stats, err := svc.ReapExpiredOpenHandleLeases(ctx, 1)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.ExpiredRefs)
	require.EqualValues(t, 1, stats.OrphanedInodesGCed)
	_, err = svc.GetAttr(ctx, expired.Inode)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.GetAttr(ctx, live.Inode)
	require.NoError(t, err)
}

func TestServiceStatFSReportsConfiguredCapacityAndFileCounts(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2},
		WithCapacity(16),
		WithMaxFiles(10),
	)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcde"))
	require.NoError(t, err)

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, testChunkSize, stats.ChunkSize)
	require.EqualValues(t, 2, stats.Files)
	require.EqualValues(t, 8, stats.FreeFiles)
	require.EqualValues(t, 16, stats.Capacity)
	require.EqualValues(t, 11, stats.Free)
}

func TestServiceStatFSReportsUnconfiguredCapacityAsUnlimited(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.Zero(t, stats.Capacity)
	require.Equal(t, uint64(math.MaxUint64), stats.Free)
	require.Equal(t, uint64(math.MaxUint64), stats.FreeFiles)
}

func TestServiceStatFSUsesUsageCounter(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(100), WithMaxFiles(100))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	elem, err := putElem(fskeys.UsageRouteKey(kv.RouteKey(fskeys.InodeKey(2))), routedFSUsageCounter{FilesAdd: 41, BytesAdd: 17})
	require.NoError(t, err)
	require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{elem}, [][]byte{elem.Key}))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 42, stats.Files)
	require.EqualValues(t, 83, stats.Free)
}

func TestServiceUsageUpdatesUseRoutedCounter(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2, 3}, WithCapacity(100))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	rec := attachRecorder(svc)
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.Len(t, rec.requests, 2)
	requireUsageRouteCounterWithTxn(t, rec.requests[1].Elems)
	require.False(t, elemTouchesKey(rec.requests[1].Elems, fskeys.UsageKey()))

	rec.requests = nil
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abc"))
	require.NoError(t, err)
	require.Len(t, rec.requests, 1)
	requireUsageRouteCounterWithTxn(t, rec.requests[0].Elems)
	require.False(t, elemTouchesKey(rec.requests[0].Elems, fskeys.UsageKey()))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 2, stats.Files)
	require.EqualValues(t, 97, stats.Free)
}

func TestServiceStatFSCombinesLegacyUsageWithRoutedDeltas(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(100), WithMaxFiles(100))

	legacy, err := putElem(fskeys.UsageKey(), FSUsage{Files: 42, Bytes: 20})
	require.NoError(t, err)
	routeKey := fskeys.UsageRouteKey(kv.RouteKey(fskeys.InodeKey(2)))
	routed, err := putElem(routeKey, routedFSUsageCounter{FilesAdd: 2, FilesSub: 1, BytesAdd: 7, BytesSub: 3})
	require.NoError(t, err)
	require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{legacy, routed}, [][]byte{fskeys.UsageKey(), routeKey}))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 43, stats.Files)
	require.EqualValues(t, 76, stats.Free)
}

func TestServiceTruncateSparseTailPreservesUsage(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	require.NoError(t, svc.Truncate(ctx, file.Inode, 2*testChunkSize))
	require.NoError(t, svc.Truncate(ctx, file.Inode, 3))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(file.Inode, file.Inode, 0), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceTruncatePagesLargeChunkDeletes(t *testing.T) {
	ctx := context.Background()
	const (
		chunkCount             = chunkDeleteTxnPageSize + 1
		largeChunkPayloadBytes = chunkCount * 4
	)
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(uint64(largeChunkPayloadBytes)))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{'x'}, largeChunkPayloadBytes)
	_, err = svc.Write(ctx, file.Inode, 0, 0, payload)
	require.NoError(t, err)

	rec := attachRecorder(svc)
	require.NoError(t, svc.Truncate(ctx, file.Inode, 0))
	require.Len(t, rec.requests, 2)
	for _, req := range rec.requests {
		require.LessOrEqual(t, len(req.ReadKeys), chunkDeleteTxnPageSize+4)
	}

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)
}

func TestServiceSizeShrinkFinishesCleanupAfterCallerCancellation(t *testing.T) {
	operations := map[string]func(context.Context, *Service, uint64) error{
		"truncate": func(ctx context.Context, svc *Service, inode uint64) error {
			return svc.Truncate(ctx, inode, 0)
		},
		"setattr": func(ctx context.Context, svc *Service, inode uint64) error {
			_, err := svc.SetAttr(ctx, inode, SetAttrMask{Size: true}, SetAttr{Size: 0})
			return err
		},
	}
	for name, shrink := range operations {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			const payloadBytes = (chunkDeleteTxnPageSize + 1) * 4
			svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(payloadBytes))
			require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
			file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
			require.NoError(t, err)
			_, err = svc.Write(ctx, file.Inode, 0, 0, bytes.Repeat([]byte{'x'}, payloadBytes))
			require.NoError(t, err)

			inner, ok := svc.dispatch.(*testCoordinator)
			require.True(t, ok)
			svc.dispatch = &afterSuccessfulDispatchCoordinator{
				inner: inner,
				after: func(context.Context) {
					cancel()
				},
			}
			require.NoError(t, shrink(ctx, svc, file.Inode))
			require.ErrorIs(t, ctx.Err(), context.Canceled)

			stats, err := svc.StatFS(context.Background(), RootInode)
			require.NoError(t, err)
			require.EqualValues(t, 0, stats.Capacity-stats.Free)
			meta, err := svc.inodeAt(context.Background(), file.Inode, svc.store.LastCommitTS())
			require.NoError(t, err)
			assertChunkPrefixEmpty(t, context.Background(), svc.store, fskeys.ChunkPrefix(meta.HomeSlot, file.Inode))
		})
	}
}

func TestServiceUnlinkLargeFileSucceedsAfterPagedDeleteFailure(t *testing.T) {
	ctx := context.Background()
	const (
		chunkCount             = chunkDeleteTxnPageSize + 1
		largeChunkPayloadBytes = chunkCount * 4
	)
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(uint64(largeChunkPayloadBytes)))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("large"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{'x'}, largeChunkPayloadBytes)
	_, err = svc.Write(ctx, file.Inode, 0, 0, payload)
	require.NoError(t, err)

	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	failCleanup := &failNthDispatchCoordinator{
		inner:  inner,
		failAt: 3,
		err:    context.Canceled,
	}
	svc.dispatch = failCleanup

	err = svc.Unlink(ctx, RootInode, []byte("large"))
	require.NoError(t, err)
	_, err = svc.Resolve(ctx, RootInode, []byte("large"))
	require.ErrorIs(t, err, ErrNotFound)
	raw, err := svc.store.GetAt(ctx, fskeys.InodeKey(file.Inode), svc.store.LastCommitTS())
	require.NoError(t, err)
	meta, err := decodeJSON[InodeMeta](raw)
	require.NoError(t, err)
	require.True(t, meta.Orphaned)
	require.Zero(t, meta.Nlink)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(file.Inode, file.Inode, chunkDeleteTxnPageSize), svc.store.LastCommitTS())
	require.NoError(t, err)

	svc.dispatch = inner
	stats, err := svc.ReapExpiredOpenHandleLeases(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.OrphanedInodesGCed)
	_, err = svc.store.GetAt(ctx, fskeys.InodeKey(file.Inode), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(file.Inode, file.Inode, chunkDeleteTxnPageSize), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	fsStats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 1, fsStats.Files)
	require.EqualValues(t, 0, fsStats.Capacity-fsStats.Free)
}

func TestServiceRenameLargeReplacementSucceedsAfterPagedDeleteFailure(t *testing.T) {
	ctx := context.Background()
	const (
		chunkCount             = chunkDeleteTxnPageSize + 1
		largeChunkPayloadBytes = chunkCount * 4
	)
	svc := newTestServiceWithOptions(t, []uint64{2, 3}, WithCapacity(uint64(largeChunkPayloadBytes)))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	source, err := svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	target, err := svc.Create(ctx, RootInode, []byte("target"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{'x'}, largeChunkPayloadBytes)
	_, err = svc.Write(ctx, target.Inode, 0, 0, payload)
	require.NoError(t, err)

	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &failNthDispatchCoordinator{
		inner:  inner,
		failAt: 2,
		err:    context.Canceled,
	}

	require.NoError(t, svc.Rename(ctx, RootInode, []byte("source"), RootInode, []byte("target")))
	_, err = svc.Resolve(ctx, RootInode, []byte("source"))
	require.ErrorIs(t, err, ErrNotFound)
	inode, err := svc.Resolve(ctx, RootInode, []byte("target"))
	require.NoError(t, err)
	require.Equal(t, source.Inode, inode)
	raw, err := svc.store.GetAt(ctx, fskeys.InodeKey(target.Inode), svc.store.LastCommitTS())
	require.NoError(t, err)
	meta, err := decodeJSON[InodeMeta](raw)
	require.NoError(t, err)
	require.True(t, meta.Orphaned)
	require.Zero(t, meta.Nlink)

	svc.dispatch = inner
	stats, err := svc.ReapExpiredOpenHandleLeases(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 1, stats.OrphanedInodesGCed)
	_, err = svc.store.GetAt(ctx, fskeys.InodeKey(target.Inode), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceDeleteChunkPagesStopsAfterWriteConflictRetryLimit(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcd"))
	require.NoError(t, err)
	forceInodeSizeZeroForTest(t, ctx, svc, file.Inode)

	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &writeConflictCoordinator{inner: inner}
	svc.dispatch = conflicts

	err = svc.deleteChunkPages(ctx, &chunkDeletePlan{
		homeSlot:        file.Inode,
		inode:           file.Inode,
		chunkSize:       testChunkSize,
		start:           fskeys.ChunkPrefix(file.Inode, file.Inode),
		protectLiveSize: true,
	})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Contains(t, err.Error(), "filesystem chunk delete retry limit reached")
	require.Equal(t, chunkDeleteWriteConflictRetries, conflicts.calls)
}

func TestServiceDeleteChunkPagesContinuesUnprotectedAfterRetryLimit(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(16))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcd"))
	require.NoError(t, err)

	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	conflicts := &transientWriteConflictCoordinator{
		inner:     inner,
		remaining: chunkDeleteWriteConflictRetries + 1,
	}
	svc.dispatch = conflicts

	err = svc.deleteChunkPages(ctx, &chunkDeletePlan{
		homeSlot:  file.Inode,
		inode:     file.Inode,
		chunkSize: testChunkSize,
		start:     fskeys.ChunkPrefix(file.Inode, file.Inode),
	})
	require.NoError(t, err)
	require.Greater(t, conflicts.calls, chunkDeleteWriteConflictRetries)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(file.Inode, file.Inode, 0), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceTruncateReclaimsStaleChunksAtCurrentEOF(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(32))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, bytes.Repeat([]byte{'x'}, int(3*testChunkSize)))
	require.NoError(t, err)
	forceInodeSizeZeroForTest(t, ctx, svc, file.Inode)

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 3*testChunkSize, stats.Capacity-stats.Free)

	require.NoError(t, svc.Truncate(ctx, file.Inode, 0))
	stats, err = svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)
	meta, err := svc.inodeAt(ctx, file.Inode, svc.store.LastCommitTS())
	require.NoError(t, err)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(meta.HomeSlot, file.Inode, 0), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceTruncateRejectsHomeThatStartsMigratingDuringStaleCleanup(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(32))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, bytes.Repeat([]byte{'x'}, int(3*testChunkSize)))
	require.NoError(t, err)
	forceInodeSizeZeroForTest(t, ctx, svc, file.Inode)

	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	hook := &afterSuccessfulDispatchCoordinator{inner: inner}
	hook.after = func(ctx context.Context) {
		svc.dispatch = inner
		setMigratingHomeForTest(t, ctx, svc, file.Inode, file.Inode+1)
		svc.dispatch = hook
	}
	svc.dispatch = hook

	err = svc.Truncate(ctx, file.Inode, 0)
	require.ErrorIs(t, err, ErrStaleHome)
	require.True(t, hook.called)
}

func TestServiceTruncateShrinkReclaimsStaleChunksPastCurrentEOF(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(32))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, bytes.Repeat([]byte{'x'}, int(3*testChunkSize)))
	require.NoError(t, err)
	forceInodeSizeForTest(t, ctx, svc, file.Inode, 2*testChunkSize)

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 3*testChunkSize, stats.Capacity-stats.Free)

	require.NoError(t, svc.Truncate(ctx, file.Inode, testChunkSize))
	stats, err = svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, testChunkSize, stats.Capacity-stats.Free)

	meta, err := svc.inodeAt(ctx, file.Inode, svc.store.LastCommitTS())
	require.NoError(t, err)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(meta.HomeSlot, file.Inode, 1), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = svc.store.GetAt(ctx, fskeys.ChunkKey(meta.HomeSlot, file.Inode, 2), svc.store.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestServiceTruncateGrowReclaimsStaleChunksBeforeExpose(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(32))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("stale-data"))
	require.NoError(t, err)
	forceInodeSizeZeroForTest(t, ctx, svc, file.Inode)

	require.NoError(t, svc.Truncate(ctx, file.Inode, 8))
	got, err := svc.Read(ctx, file.Inode, 0, 0, 8)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 8), got)
	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Capacity-stats.Free)
}

func TestServiceWriteGrowReclaimsStaleChunksBeforeExpose(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(32))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("stale-data"))
	require.NoError(t, err)
	forceInodeSizeZeroForTest(t, ctx, svc, file.Inode)

	_, err = svc.Write(ctx, file.Inode, 0, 6, []byte("Z"))
	require.NoError(t, err)
	got, err := svc.Read(ctx, file.Inode, 0, 0, 7)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 'Z'}, got)
	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.Capacity-stats.Free)
}

func TestServiceCrossParentRenameReturnsEXDEV(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t, 2, 3, 4)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	dir, err := svc.Mkdir(ctx, RootInode, []byte("dir"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	_, err = svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	err = svc.Rename(ctx, RootInode, []byte("file"), dir.Inode, []byte("file"))
	require.ErrorIs(t, err, ErrCrossDevice)
}

func newTestService(t *testing.T, ids ...uint64) *Service {
	t.Helper()
	return newTestServiceWithOptions(t, ids)
}

func newTestServiceWithOptions(t *testing.T, ids []uint64, opts ...Option) *Service {
	t.Helper()
	st := store.NewMVCCStore()
	coord := &testCoordinator{st: st}
	nextID := sequenceIDAllocator(ids...)
	serviceOpts := []Option{
		WithChunkSize(testChunkSize),
		WithIDAllocator(nextID),
	}
	serviceOpts = append(serviceOpts, opts...)
	svc, err := NewService(st, coord, serviceOpts...)
	require.NoError(t, err)
	return svc
}

func attachRecorder(svc *Service) *recordingCoordinator {
	inner, ok := svc.dispatch.(*testCoordinator)
	if !ok {
		panic("attachRecorder requires a testCoordinator-backed service")
	}
	rec := &recordingCoordinator{inner: inner}
	svc.dispatch = rec
	return rec
}

func cloneKeys(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, key := range in {
		out = append(out, append([]byte(nil), key...))
	}
	return out
}

func forceInodeSizeZeroForTest(t *testing.T, ctx context.Context, svc *Service, inode uint64) {
	t.Helper()
	forceInodeSizeForTest(t, ctx, svc, inode, 0)
}

func forceInodeSizeForTest(t *testing.T, ctx context.Context, svc *Service, inode uint64, size uint64) {
	t.Helper()
	ts := svc.store.LastCommitTS()
	meta, err := svc.inodeAt(ctx, inode, ts)
	require.NoError(t, err)
	meta.Size = size
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	require.NoError(t, err)
	require.NoError(t, svc.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{fskeys.InodeKey(inode)}))
}

func keyInSet(keys [][]byte, want []byte) bool {
	for _, key := range keys {
		if bytes.Equal(key, want) {
			return true
		}
	}
	return false
}

func elemTouchesKey(elems []*kv.Elem[kv.OP], want []byte) bool {
	for _, elem := range elems {
		if bytes.Equal(elem.Key, want) {
			return true
		}
	}
	return false
}

func requireUsageRouteCounterWithTxn(t *testing.T, elems []*kv.Elem[kv.OP]) {
	t.Helper()
	var txnRoute []byte
	for _, elem := range elems {
		if elem == nil || fskeys.IsUsageRouteKey(elem.Key) || bytes.Equal(elem.Key, fskeys.UsageKey()) {
			continue
		}
		txnRoute = kv.RouteKey(elem.Key)
		break
	}
	require.NotNil(t, txnRoute)

	for _, elem := range elems {
		if elem == nil || !fskeys.IsUsageRouteKey(elem.Key) {
			continue
		}
		require.Equal(t, txnRoute, kv.RouteKey(elem.Key))
		return
	}
	require.Fail(t, "missing routed usage counter")
}

func sequenceIDAllocator(ids ...uint64) func() (uint64, error) {
	next := append([]uint64(nil), ids...)
	return func() (uint64, error) {
		if len(next) == 0 {
			return 0, ErrInodeCollisionLimit
		}
		id := next[0]
		next = next[1:]
		return id, nil
	}
}

func containsDirent(entries []Dirent, name []byte) bool {
	for _, entry := range entries {
		if bytes.Equal(entry.Name, name) {
			return true
		}
	}
	return false
}

// Root initialization runs against a store that has never committed anything,
// so LastCommitTS() reports the zero watermark. kv.BeginReadTimestampThrough
// rejects a zero read timestamp outright once Phase-D is required, so a zero
// start timestamp leaving dispatchTxn means the filesystem cannot bring up its
// root inode on an otherwise valid empty cluster.
func TestDispatchTxnNormalizesEmptyStoreReadTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	inner := &testCoordinator{st: st}
	rec := &recordingCoordinator{inner: inner}
	svc, err := NewService(st, rec, WithChunkSize(testChunkSize))
	require.NoError(t, err)

	require.Zero(t, st.LastCommitTS(), "the fixture must start from the empty watermark")
	require.NoError(t, svc.initializeRootOnce(ctx, testDirMode, 0, 0))

	require.NotEmpty(t, rec.requests)
	root := rec.requests[0]
	require.True(t, root.IsTxn)
	require.NotZero(t, root.StartTS,
		"a zero start timestamp is rejected by Phase-D and also hands start-timestamp"+
			" selection to the coordinator, which defeats the OCC read-set check")
	require.Equal(t, uint64(emptyStoreReadTS), root.StartTS)

	// The root really was created, so the normalized snapshot is a usable one.
	meta, err := svc.inodeAt(ctx, RootInode, st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, RootInode, meta.Inode)
}

// A non-empty watermark must pass through untouched: the normalization exists
// only for the empty-store sentinel.
func TestDispatchTxnKeepsNonEmptyReadTimestamp(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(emptyStoreReadTS), normalizeEmptyStoreReadTS(0))
	for _, ts := range []uint64{1, 2, 1 << 20, ^uint64(0)} {
		require.Equal(t, ts, normalizeEmptyStoreReadTS(ts))
	}
}
