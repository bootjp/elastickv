package fuseadapter_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

type collidingMountCore struct {
	fuseadapter.Core
	ready           chan struct{}
	createMu        sync.Mutex
	creates         atomic.Int32
	collisions      atomic.Int32
	interruptFlush  atomic.Bool
	interrupted     atomic.Int32
	released        atomic.Int32
	releaseFailures atomic.Int32
}

func (c *collidingMountCore) Flush(ctx context.Context, inode, fh uint64) error {
	if err := c.Core.Flush(ctx, inode, fh); err != nil {
		return err
	}
	if c.interruptFlush.CompareAndSwap(true, false) {
		c.interrupted.Add(1)
		// Core reports backend cancellation; Adapter.Errno translates it to
		// EINTR at the kernel boundary. A raw errno is not a Core error contract.
		return context.Canceled
	}
	return nil
}

func (c *collidingMountCore) Release(ctx context.Context, inode, fh uint64, client []byte) error {
	err := c.Core.Release(ctx, inode, fh, client)
	if err != nil {
		c.releaseFailures.Add(1)
	}
	c.released.Add(1)
	return err
}

func (c *collidingMountCore) Create(ctx context.Context, parent uint64, name []byte, opts filesystem.CreateOptions) (filesystem.CreateResult, error) {
	if attempt := c.creates.Add(1); attempt <= 2 {
		if attempt == 2 {
			close(c.ready)
		}
		select {
		case <-c.ready:
		case <-time.After(5 * time.Second):
			return filesystem.CreateResult{}, context.DeadlineExceeded
		}
	}
	// Exercise a definite EEXIST, not a concurrent OCC failure whose commit
	// outcome the adapter cannot safely replay. The initial barrier above
	// forces both mounts into FUSE_CREATE before either backend call finishes.
	c.createMu.Lock()
	defer c.createMu.Unlock()
	result, err := c.Core.Create(ctx, parent, name, opts)
	if errors.Is(err, filesystem.ErrExists) {
		c.collisions.Add(1)
	}
	return result, err
}

func TestCollidingMountCoreInterruptedFlush(t *testing.T) {
	t.Parallel()
	backend := &fakeCore{}
	core := &collidingMountCore{Core: backend}
	core.interruptFlush.Store(true)
	adapter := fuseadapter.New(core, []byte("unit-client"), fuseadapter.WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	raw := fuseadapter.NewRawFileSystem(adapter)
	input := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}

	require.Equal(t, fuse.Status(syscall.EINTR), raw.Flush(nil, input))
	require.Equal(t, 1, backend.flushCalls)
	require.EqualValues(t, 1, core.interrupted.Load())
	require.False(t, core.interruptFlush.Load())

	// Injection is one-shot. Later success and real backend failures must
	// remain distinguishable from the injected cancellation.
	require.Equal(t, fuse.OK, raw.Flush(nil, input))
	require.Equal(t, 2, backend.flushCalls)
	backend.flushErr = errors.New("backend I/O failure")
	require.Equal(t, fuse.Status(syscall.EIO), raw.Flush(nil, input))
	require.Equal(t, 3, backend.flushCalls)
	require.EqualValues(t, 1, core.interrupted.Load())
}

// Only Flush and lease refresh are used by this request. Embedding Core
// makes an unexpected backend operation fail instead of silently succeeding.
type fakeCore struct {
	fuseadapter.Core
	flushCalls int
	flushErr   error
}

func (c *fakeCore) Flush(context.Context, uint64, uint64) error {
	c.flushCalls++
	return c.flushErr
}

func (*fakeCore) RefreshOpenHandleLease(context.Context, uint64, uint64, []byte) error {
	return nil
}
