package fuseadapter

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

type blockingWriteCore struct {
	Core
	started chan struct{}
	release chan struct{}
}

type blockingMkdirCore struct {
	Core
	started chan struct{}
	release chan struct{}
}

func (c *blockingMkdirCore) Mkdir(ctx context.Context, parent uint64, name []byte, opts filesystem.CreateOptions) (filesystem.CreateResult, error) {
	close(c.started)
	select {
	case <-c.release:
		return c.Core.Mkdir(ctx, parent, name, opts)
	case <-ctx.Done():
		return filesystem.CreateResult{}, ctx.Err()
	}
}

func (c *blockingWriteCore) Write(ctx context.Context, inode, fh, offset uint64, data []byte) (int, error) {
	close(c.started)
	select {
	case <-c.release:
		return c.Core.Write(ctx, inode, fh, offset, data)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestFsyncWaitsForEarlierWrite(t *testing.T) {
	core := &fakeCore{}
	blocked := &blockingWriteCore{Core: core, started: make(chan struct{}), release: make(chan struct{})}
	adapter := New(blocked, []byte("client"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	writeDone := make(chan syscall.Errno, 1)
	go func() {
		_, errno := adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
		writeDone <- errno
	}()
	<-blocked.started
	fsyncDone := make(chan syscall.Errno, 1)
	go func() { fsyncDone <- adapter.Fsync(t.Context(), 7, 9, false) }()
	testutil.NoRecv(t, fsyncDone, 30*time.Millisecond, "fsync overtook an earlier write")
	close(blocked.release)
	require.Zero(t, <-writeDone)
	require.Zero(t, <-fsyncDone)
	require.True(t, core.fsyncCalled)
}

func TestFsyncRetainsUnknownPredecessorError(t *testing.T) {
	core := &fakeCore{writeErr: kv.ErrTransactionOutcomeUnknown}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
	require.Equal(t, syscall.EIO, errno)
	require.Equal(t, syscall.EIO, adapter.Fsync(t.Context(), 7, 9, false))
	core.writeErr = nil
	_, errno = adapter.Write(t.Context(), 7, 9, 0, []byte("later"))
	require.Equal(t, syscall.EIO, errno)
	require.False(t, core.fsyncCalled)
}

func TestMutationTrackerRetainsOtherUnknownAfterOneResolution(t *testing.T) {
	var tracker mutationTracker
	tracker.markUnknown(7)
	tracker.markUnknown(7)
	tracker.resolveUnknown(7)
	_, errno := tracker.begin(t.Context(), 7)
	require.Equal(t, syscall.EIO, errno)
	tracker.resolveUnknown(7)
	ticket, errno := tracker.begin(t.Context(), 7)
	require.Zero(t, errno)
	tracker.end(ticket, false)
}

func TestOrdinaryWriteEIODoesNotPoisonLaterMutations(t *testing.T) {
	core := &fakeCore{writeErr: errors.New("storage unavailable")}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Write(t.Context(), 7, 9, 0, []byte("first"))
	require.Equal(t, syscall.EIO, errno)
	core.writeErr = nil
	_, errno = adapter.Write(t.Context(), 7, 9, 0, []byte("second"))
	require.Zero(t, errno)
	require.Zero(t, adapter.Fsync(t.Context(), 7, 9, false))
}

func TestDirectoryFsyncWaitsForEarlierNamespaceMutation(t *testing.T) {
	core := &fakeCore{}
	blocked := &blockingMkdirCore{Core: core, started: make(chan struct{}), release: make(chan struct{})}
	adapter := New(blocked, []byte("client"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	mkdirDone := make(chan syscall.Errno, 1)
	go func() {
		_, errno := adapter.Mkdir(t.Context(), filesystem.RootInode, []byte("dir"), filesystem.CreateOptions{})
		mkdirDone <- errno
	}()
	<-blocked.started
	fsyncDone := make(chan syscall.Errno, 1)
	go func() { fsyncDone <- adapter.FsyncDirectory(t.Context(), filesystem.RootInode) }()
	testutil.NoRecv(t, fsyncDone, 30*time.Millisecond, "directory fsync overtook an earlier mkdir")
	close(blocked.release)
	require.Zero(t, <-mkdirDone)
	require.Zero(t, <-fsyncDone)
	require.True(t, core.fsyncCalled)
}

func TestMutationTrackerOrdersSameInodeAndReleasesCanceledWaiter(t *testing.T) {
	var tracker mutationTracker
	first, errno := tracker.begin(t.Context(), 7)
	require.Zero(t, errno)
	waitCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	secondDone := make(chan syscall.Errno, 1)
	go func() {
		_, result := tracker.begin(waitCtx, 7)
		secondDone <- result
	}()
	testutil.NoRecv(t, secondDone, 30*time.Millisecond, "second mutation overtook its predecessor")
	cancel()
	require.NotZero(t, <-secondDone)
	tracker.end(first, false)
	third, errno := tracker.begin(t.Context(), 7)
	require.Zero(t, errno)
	tracker.end(third, false)
	require.Zero(t, tracker.count)
}
