//go:build linux

// The kernel-mount tests drive a real /dev/fuse mount and read Linux statfs
// fields, so they only build on Linux; elsewhere the package's portable
// tests still run.

package fuseadapter_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	e2eChunkSize   uint64 = 8
	e2eRootMode    uint32 = 0o755
	e2eRequireFuse        = "ELASTICKV_REQUIRE_FUSE"
)

// mountMemoryFilesystem attaches an in-memory filesystem.Service to a kernel
// FUSE mount. Environments without a usable /dev/fuse skip unless
// ELASTICKV_REQUIRE_FUSE is set, in which case a mount failure is fatal.
func mountMemoryFilesystem(t *testing.T) string {
	t.Helper()
	mountPoint := t.TempDir()
	mountMemoryFilesystemAt(t, mountPoint)
	return mountPoint
}

// mountMemoryFilesystemAt mounts a fresh in-memory filesystem at an existing
// directory and unmounts it when the test ends.
func mountMemoryFilesystemAt(t *testing.T, mountPoint string) {
	t.Helper()
	mountMemoryFilesystemServer(t, mountPoint)
}

// e2eMount is a kernel mount owned by a test. unmount is idempotent so a
// test may unmount early and let the cleanup run harmlessly afterwards.
type e2eMount struct {
	server    *fuseadapter.Server
	serveDone <-chan struct{}
	unmount   func() error
}

func mountMemoryFilesystemServer(t *testing.T, mountPoint string) *e2eMount {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFail(t, "no /dev/fuse: %v", err)
	}
	return mountMemoryServiceAt(t, newMemoryServiceForMount(t), "e2e-mount", mountPoint)
}

func newMemoryServiceForMount(t *testing.T) *filesystem.Service {
	return newMemoryServiceForMountWithChunkSize(t, e2eChunkSize)
}

func newMemoryServiceForMountWithChunkSize(t *testing.T, chunkSize uint64) *filesystem.Service {
	t.Helper()
	svc, err := fstest.NewMemoryService(filesystem.WithChunkSize(chunkSize))
	require.NoError(t, err)
	info, err := os.Stat(t.TempDir())
	require.NoError(t, err)
	owner, ok := info.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.NoError(t, svc.InitializeRoot(t.Context(), e2eRootMode, owner.Uid, owner.Gid))
	return svc
}

func mountMemoryService(t *testing.T, svc fuseadapter.Core, clientID string) string {
	t.Helper()
	mountPoint := t.TempDir()
	mountMemoryServiceAt(t, svc, clientID, mountPoint)
	return mountPoint
}

func mountMemoryServiceAt(t *testing.T, svc fuseadapter.Core, clientID, mountPoint string) *e2eMount {
	t.Helper()
	adapter := fuseadapter.New(svc, []byte(clientID))
	server, err := fuseadapter.Mount(mountPoint, adapter, &fuse.MountOptions{DirectMount: os.Geteuid() == 0})
	if err != nil {
		adapter.Close()
		skipOrFail(t, "FUSE mount unavailable: %v", err)
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		<-serveDone
		skipOrFail(t, "FUSE mount did not come up: %v", err)
	}
	mount := &e2eMount{server: server, serveDone: serveDone, unmount: sync.OnceValue(server.Unmount)}
	t.Cleanup(func() {
		require.NoError(t, mount.unmount())
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
			t.Error("FUSE server did not stop after unmount")
		}
	})
	return mount
}

func TestKernelMountConcurrentCreateAcrossMounts(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		skipOrFail(t, "no /dev/fuse: %v", err)
	}
	svc := &collidingMountCore{Core: newMemoryServiceForMount(t), ready: make(chan struct{})}

	mountA := mountMemoryService(t, svc, "e2e-mount-a")
	mountB := mountMemoryService(t, svc, "e2e-mount-b")
	for i := range 32 {
		name := "shared-" + strconv.Itoa(i)
		pathA := filepath.Join(mountA, name)
		pathB := filepath.Join(mountB, name)
		_, err := os.Stat(pathA)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(pathB)
		require.ErrorIs(t, err, os.ErrNotExist)

		start := make(chan struct{})
		type openResult struct {
			file *os.File
			err  error
		}
		results := make(chan openResult, 2)
		for _, path := range []string{pathA, pathB} {
			go func() {
				<-start
				file, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
				results <- openResult{file: file, err: openErr}
			}()
		}
		close(start)
		first, second := <-results, <-results
		assertOneCreatedDescriptor(t, first.file, second.file)
		if first.file != nil {
			require.NoError(t, first.file.Close())
		}
		if second.file != nil {
			require.NoError(t, second.file.Close())
		}
		require.NoError(t, first.err)
		require.NoError(t, second.err)
		require.FileExists(t, pathA)
		require.FileExists(t, pathB)
	}
	require.Positive(t, svc.collisions.Load(), "must exercise an actual FUSE_CREATE collision, not just existing-file Open")
}

func assertOneCreatedDescriptor(t *testing.T, first, second *os.File) {
	t.Helper()
	if first == nil || second == nil {
		return // The caller reports the underlying open errors.
	}
	a, err := unix.FcntlInt(first.Fd(), unix.F_CREATED_QUERY, 0)
	if errors.Is(err, syscall.EINVAL) {
		return // Older kernels do not implement F_CREATED_QUERY.
	}
	require.NoError(t, err)
	b, err := unix.FcntlInt(second.Fd(), unix.F_CREATED_QUERY, 0)
	require.NoError(t, err)
	require.Equal(t, 1, a+b, "only the actual creator may report a newly created descriptor")
}

type collidingMountCore struct {
	fuseadapter.Core
	ready      chan struct{}
	createMu   sync.Mutex
	creates    atomic.Int32
	collisions atomic.Int32
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

func skipOrFail(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv(e2eRequireFuse) != "" {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

func TestKernelMountFileLifecycle(t *testing.T) {
	mountPoint := mountMemoryFilesystem(t)

	path := filepath.Join(mountPoint, "hello.txt")
	payload := []byte("hello elastickv filesystem via fuse")
	require.NoError(t, os.WriteFile(path, payload, 0o600))

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, len(payload), info.Size())
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.False(t, info.IsDir())

	// Overwriting with shorter content must truncate through SETATTR.
	short := []byte("short")
	require.NoError(t, os.WriteFile(path, short, 0o600))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, short, got)

	// Partial overwrite across the chunk boundary with pwrite.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("ABCDEFGHIJ"), 3)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("shoABCDEFGHIJ"), got)

	// Sparse extension via truncate reads back as zeros.
	require.NoError(t, os.Truncate(path, 20))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, append([]byte("shoABCDEFGHIJ"), make([]byte, 7)...), got)

	require.NoError(t, os.Chmod(path, 0o640))
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())

	renamed := filepath.Join(mountPoint, "renamed.txt")
	require.NoError(t, os.Rename(path, renamed))
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	got, err = os.ReadFile(renamed)
	require.NoError(t, err)
	require.Len(t, got, 20)

	require.NoError(t, os.Remove(renamed))
	_, err = os.Stat(renamed)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestKernelMountDirectoryOperations(t *testing.T) {
	mountPoint := mountMemoryFilesystem(t)

	dir := filepath.Join(mountPoint, "dir")
	require.NoError(t, os.Mkdir(dir, 0o750))
	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, os.FileMode(0o750), info.Mode().Perm())

	require.ErrorIs(t, os.Mkdir(dir, 0o750), os.ErrExist)

	want := []string{"c", "a", "b"}
	for _, name := range want {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600))
	}
	sort.Strings(want)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
		require.False(t, entry.IsDir())
	}
	require.Equal(t, want, names)

	err = os.Remove(dir)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ENOTEMPTY)

	require.NoError(t, os.RemoveAll(dir))
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)

	root, err := os.ReadDir(mountPoint)
	require.NoError(t, err)
	require.Empty(t, root)
}

func TestKernelMountReaddirPagesLargeDirectory(t *testing.T) {
	mountPoint := mountMemoryFilesystem(t)

	const count = 300
	want := make([]string, 0, count)
	for i := range count {
		name := "entry-" + strconv.Itoa(i)
		require.NoError(t, os.WriteFile(filepath.Join(mountPoint, name), nil, 0o600))
		want = append(want, name)
	}
	sort.Strings(want)

	entries, err := os.ReadDir(mountPoint)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.Equal(t, want, names)
}

func TestKernelMountUnlinkWhileOpenKeepsDataReadable(t *testing.T) {
	mountPoint := mountMemoryFilesystem(t)

	path := filepath.Join(mountPoint, "open.txt")
	payload := []byte("still here")
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	f, err := os.Open(path)
	require.NoError(t, err)

	require.NoError(t, os.Remove(path))
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)

	got := make([]byte, len(payload))
	n, err := f.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, payload, got[:n])
	require.NoError(t, f.Close())
}

func TestKernelMountUnsupportedOperationsReturnErrno(t *testing.T) {
	mountPoint := mountMemoryFilesystem(t)

	path := filepath.Join(mountPoint, "target")
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))

	err := os.Symlink(path, filepath.Join(mountPoint, "sym"))
	require.ErrorIs(t, err, syscall.EOPNOTSUPP)

	err = os.Link(path, filepath.Join(mountPoint, "hard"))
	require.ErrorIs(t, err, syscall.EOPNOTSUPP)

	err = syscall.Mkfifo(filepath.Join(mountPoint, "fifo"), 0o644)
	require.ErrorIs(t, err, syscall.EOPNOTSUPP)
}

func TestKernelMountStatfsReportsNameLimit(t *testing.T) {
	mountPoint := mountMemoryFilesystem(t)

	var stat syscall.Statfs_t
	require.NoError(t, syscall.Statfs(mountPoint, &stat))
	require.NotZero(t, stat.Bsize)
	require.EqualValues(t, 255, stat.Namelen)
}
