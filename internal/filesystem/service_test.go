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
