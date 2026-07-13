package fuseadapter

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/store"
	cerrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestErrnoMapsFilesystemErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{name: "nil", err: nil, want: 0},
		{name: "not found", err: filesystem.ErrNotFound, want: syscall.ENOENT},
		{name: "exists", err: filesystem.ErrExists, want: syscall.EEXIST},
		{name: "not dir", err: filesystem.ErrNotDir, want: syscall.ENOTDIR},
		{name: "is dir", err: filesystem.ErrIsDir, want: syscall.EISDIR},
		{name: "not empty", err: filesystem.ErrNotEmpty, want: syscall.ENOTEMPTY},
		{name: "cross device", err: filesystem.ErrCrossDevice, want: syscall.EXDEV},
		{name: "invalid", err: filesystem.ErrInvalid, want: syscall.EINVAL},
		{name: "unsupported", err: filesystem.ErrUnsupported, want: syscall.EOPNOTSUPP},
		{name: "write conflict", err: cerrors.Wrap(store.ErrWriteConflict, "txn"), want: syscall.EAGAIN},
		{name: "canceled", err: context.Canceled, want: syscall.EINTR},
		{name: "deadline", err: context.DeadlineExceeded, want: syscall.ETIMEDOUT},
		{name: "unknown", err: errors.New("boom"), want: syscall.EIO},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, Errno(tt.err))
		})
	}
}

func TestAdapterLookupResolvesThenStatsInode(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{
		resolveInode: 42,
		stat: filesystem.Stat{
			Inode:      42,
			Generation: 1,
			Type:       filesystem.TypeFile,
			Mode:       0o644,
			Size:       5,
		},
	}
	adapter := New(core, []byte("client-a"))
	t.Cleanup(adapter.Close)

	stat, errno := adapter.Lookup(ctx, filesystem.RootInode, []byte("name"))
	require.Zero(t, errno)
	require.EqualValues(t, 42, stat.Inode)
	require.EqualValues(t, filesystem.RootInode, core.resolveParent)
	require.Equal(t, []byte("name"), core.resolveName)
	require.EqualValues(t, 42, core.getAttrInode)
}

func TestAdapterCreateAndReleaseUseClientID(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{
		createResult: filesystem.CreateResult{Inode: 7, FH: 9},
	}
	adapter := New(core, []byte("fuse-session"))
	t.Cleanup(adapter.Close)

	result, errno := adapter.Create(ctx, filesystem.RootInode, []byte("file"), filesystem.CreateOptions{
		Mode: 0o644,
		UID:  1000,
		GID:  1000,
	})
	require.Zero(t, errno)
	require.EqualValues(t, 7, result.Inode)
	require.Equal(t, []byte("fuse-session"), core.createOpts.ClientID)

	errno = adapter.Release(ctx, result.Inode, result.FH)
	require.Zero(t, errno)
	require.EqualValues(t, 7, core.releaseInode)
	require.EqualValues(t, 9, core.releaseFH)
	require.Equal(t, []byte("fuse-session"), core.releaseClientID)
}

func TestAdapterUsesConfiguredReaddirLimit(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{
		readdirResult: filesystem.ReaddirResult{
			Entries: []filesystem.Dirent{{Name: []byte("."), Inode: filesystem.RootInode}},
		},
	}
	adapter := New(core, []byte("client-a"), WithReaddirLimit(4))
	t.Cleanup(adapter.Close)

	result, errno := adapter.Readdir(ctx, filesystem.RootInode, "cookie")
	require.Zero(t, errno)
	require.Len(t, result.Entries, 1)
	require.EqualValues(t, filesystem.RootInode, core.readdirInode)
	require.Equal(t, "cookie", core.readdirCookie)
	require.Equal(t, 4, core.readdirLimit)
}

func TestAdapterRefreshesHandleLeaseBeforeRead(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{readData: []byte("payload")}
	adapter := New(core, []byte("fuse-session"))
	t.Cleanup(adapter.Close)

	data, errno := adapter.Read(ctx, 7, 9, 0, 16)
	require.Zero(t, errno)
	require.Equal(t, []byte("payload"), data)
	require.EqualValues(t, 7, core.refreshInode)
	require.EqualValues(t, 9, core.refreshFH)
	require.Equal(t, []byte("fuse-session"), core.refreshClientID)
	require.True(t, core.readCalled)
}

func TestAdapterStopsReadWhenLeaseRefreshFails(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{refreshErr: filesystem.ErrNotFound}
	adapter := New(core, []byte("fuse-session"))
	t.Cleanup(adapter.Close)

	data, errno := adapter.Read(ctx, 7, 9, 0, 16)
	require.Equal(t, syscall.ENOENT, errno)
	require.Nil(t, data)
	require.False(t, core.readCalled)
}

func TestUnsupportedOperationsReturnExplicitErrno(t *testing.T) {
	adapter := New(&fakeCore{}, []byte("client-a"))
	t.Cleanup(adapter.Close)

	require.Equal(t, syscall.EOPNOTSUPP, adapter.Link(context.Background()))
	require.Equal(t, syscall.EOPNOTSUPP, adapter.Symlink(context.Background()))
	require.Equal(t, syscall.ENOSYS, adapter.Readlink(context.Background()))
	require.Equal(t, syscall.EOPNOTSUPP, adapter.FileLock(context.Background()))
}

func TestAdapterKeepsOpenHandlesAliveWhileIdle(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{
		openFH:    9,
		refreshCh: make(chan openHandleRefresh, 4),
	}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(10*time.Millisecond))
	t.Cleanup(adapter.Close)

	fh, errno := adapter.Open(ctx, 7)
	require.Zero(t, errno)
	require.EqualValues(t, 9, fh)
	require.EqualValues(t, 7, core.openInode)
	require.Equal(t, []byte("fuse-session"), core.openClientID)

	select {
	case got := <-core.refreshCh:
		require.EqualValues(t, 7, got.inode)
		require.EqualValues(t, 9, got.fh)
		require.Equal(t, []byte("fuse-session"), got.clientID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for open handle keepalive")
	}
}

type openHandleRefresh struct {
	inode    uint64
	fh       uint64
	clientID []byte
}

type fakeCore struct {
	resolveParent uint64
	resolveName   []byte
	resolveInode  uint64
	resolveErr    error

	getAttrInode uint64
	stat         filesystem.Stat
	getAttrErr   error

	createOpts   filesystem.CreateOptions
	createResult filesystem.CreateResult
	createErr    error

	releaseInode    uint64
	releaseFH       uint64
	releaseClientID []byte
	releaseErr      error

	openInode    uint64
	openClientID []byte
	openFH       uint64
	openErr      error

	refreshInode    uint64
	refreshFH       uint64
	refreshClientID []byte
	refreshErr      error
	refreshCh       chan openHandleRefresh

	readCalled bool
	readData   []byte
	readErr    error

	readdirInode  uint64
	readdirCookie string
	readdirLimit  int
	readdirResult filesystem.ReaddirResult
	readdirErr    error
}

func (f *fakeCore) Resolve(_ context.Context, parent uint64, name []byte) (uint64, error) {
	f.resolveParent = parent
	f.resolveName = append([]byte(nil), name...)
	return f.resolveInode, f.resolveErr
}

func (f *fakeCore) GetAttr(_ context.Context, inode uint64) (filesystem.Stat, error) {
	f.getAttrInode = inode
	return f.stat, f.getAttrErr
}

func (*fakeCore) SetAttr(
	context.Context,
	uint64,
	filesystem.SetAttrMask,
	filesystem.SetAttr,
) (filesystem.Stat, error) {
	return filesystem.Stat{}, nil
}

func (f *fakeCore) Open(_ context.Context, inode uint64, clientID []byte) (uint64, error) {
	f.openInode = inode
	f.openClientID = append([]byte(nil), clientID...)
	return f.openFH, f.openErr
}

func (f *fakeCore) Read(context.Context, uint64, uint64, uint64, uint64) ([]byte, error) {
	f.readCalled = true
	return f.readData, f.readErr
}

func (*fakeCore) Write(context.Context, uint64, uint64, uint64, []byte) (int, error) {
	return 0, nil
}

func (*fakeCore) Flush(context.Context, uint64, uint64) error {
	return nil
}

func (*fakeCore) Fsync(context.Context, uint64, uint64, bool) error {
	return nil
}

func (f *fakeCore) Release(_ context.Context, inode uint64, fh uint64, clientID []byte) error {
	f.releaseInode = inode
	f.releaseFH = fh
	f.releaseClientID = append([]byte(nil), clientID...)
	return f.releaseErr
}

func (f *fakeCore) RefreshOpenHandleLease(_ context.Context, inode uint64, fh uint64, clientID []byte) error {
	f.refreshInode = inode
	f.refreshFH = fh
	f.refreshClientID = append([]byte(nil), clientID...)
	if f.refreshCh != nil {
		select {
		case f.refreshCh <- openHandleRefresh{
			inode:    inode,
			fh:       fh,
			clientID: append([]byte(nil), clientID...),
		}:
		default:
		}
	}
	return f.refreshErr
}

func (f *fakeCore) Create(
	_ context.Context,
	_ uint64,
	_ []byte,
	opts filesystem.CreateOptions,
) (filesystem.CreateResult, error) {
	f.createOpts = opts
	return f.createResult, f.createErr
}

func (*fakeCore) Mkdir(
	context.Context,
	uint64,
	[]byte,
	filesystem.CreateOptions,
) (filesystem.CreateResult, error) {
	return filesystem.CreateResult{}, nil
}

func (*fakeCore) Unlink(context.Context, uint64, []byte) error {
	return nil
}

func (*fakeCore) Rmdir(context.Context, uint64, []byte) error {
	return nil
}

func (*fakeCore) Rename(context.Context, uint64, []byte, uint64, []byte) error {
	return nil
}

func (f *fakeCore) Readdir(
	_ context.Context,
	inode uint64,
	cookie string,
	limit int,
) (filesystem.ReaddirResult, error) {
	f.readdirInode = inode
	f.readdirCookie = cookie
	f.readdirLimit = limit
	return f.readdirResult, f.readdirErr
}

func (*fakeCore) StatFS(context.Context, uint64) (filesystem.StatFS, error) {
	return filesystem.StatFS{}, nil
}
