package filesystem

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const (
	testRootMode  uint32 = 0o755
	testFileMode  uint32 = 0o644
	testDirMode   uint32 = 0o755
	testChunkSize uint64 = 4
)

type testCoordinator struct {
	st store.MVCCStore
}

func (c *testCoordinator) LinearizableRead(context.Context) (uint64, error) {
	return c.st.LastCommitTS(), nil
}

func (c *testCoordinator) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	commitTS := c.st.LastCommitTS() + 1
	if req.CommitTS > commitTS {
		commitTS = req.CommitTS
	}
	if req.IsTxn && commitTS <= req.StartTS {
		commitTS = req.StartTS + 1
	}
	mutations := make([]*store.KVPairMutation, 0, len(req.Elems))
	for _, elem := range req.Elems {
		switch elem.Op {
		case kv.Put:
			mutations = append(mutations, &store.KVPairMutation{
				Op:    store.OpTypePut,
				Key:   append([]byte(nil), elem.Key...),
				Value: append([]byte(nil), elem.Value...),
			})
		case kv.Del:
			mutations = append(mutations, &store.KVPairMutation{
				Op:  store.OpTypeDelete,
				Key: append([]byte(nil), elem.Key...),
			})
		case kv.DelPrefix:
			return nil, store.ErrUnknownOp
		default:
			return nil, store.ErrUnknownOp
		}
	}
	if err := c.st.ApplyMutations(ctx, mutations, req.ReadKeys, req.StartTS, commitTS); err != nil {
		return nil, err
	}
	return &kv.CoordinateResponse{CommitIndex: commitTS}, nil
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
}

func (c *transientWriteConflictCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *transientWriteConflictCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.calls++
	if c.remaining > 0 {
		c.remaining--
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
	require.Len(t, rec.requests, 1)
	require.True(t, keyInSet(rec.requests[0].ReadKeys, fskeys.RefKey(file.Inode, []byte("client-a"), file.FH)))
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
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.InodeKey(file.Inode)))
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.RefFenceKey(file.Inode)))
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.UsageKey()))

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
	require.True(t, elemTouchesKey(rec.requests[0].Elems, fskeys.UsageKey()))
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

func TestServiceStatFSUsesUsageCounter(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2}, WithCapacity(100), WithMaxFiles(100))

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	elem, err := putElem(fskeys.UsageKey(), FSUsage{Files: 42, Bytes: 17})
	require.NoError(t, err)
	require.NoError(t, svc.dispatchTxn(ctx, svc.store.LastCommitTS(), []*kv.Elem[kv.OP]{elem}, [][]byte{fskeys.UsageKey()}))

	stats, err := svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.EqualValues(t, 42, stats.Files)
	require.EqualValues(t, 83, stats.Free)
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

func TestServiceUnlinkLargeFileGCIsRecoverableAfterPagedDeleteFailure(t *testing.T) {
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
	failSecond := &failNthDispatchCoordinator{
		inner:  inner,
		failAt: 2,
		err:    context.Canceled,
	}
	svc.dispatch = failSecond

	err = svc.Unlink(ctx, RootInode, []byte("large"))
	require.ErrorIs(t, err, context.Canceled)
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
	ts := svc.store.LastCommitTS()
	meta, err := svc.inodeAt(ctx, inode, ts)
	require.NoError(t, err)
	meta.Size = 0
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
