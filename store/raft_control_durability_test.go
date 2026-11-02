package store

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

type controlSyncFS struct {
	vfs.FS
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

type controlSyncFile struct {
	vfs.File
	fs *controlSyncFS
}

func (f *controlSyncFS) wrap(name string, file vfs.File, err error) (vfs.File, error) {
	if err != nil || !strings.HasSuffix(name, ".log") {
		return file, err
	}
	return &controlSyncFile{File: file, fs: f}, nil
}

func (f *controlSyncFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, category)
	return f.wrap(name, file, err)
}

func (f *controlSyncFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, category)
	return f.wrap(newname, file, err)
}

func (f *controlSyncFile) wait() {
	if f.fs.armed.Load() {
		f.fs.once.Do(func() { close(f.fs.entered) })
		<-f.fs.release
	}
}

func (f *controlSyncFile) Sync() error {
	f.wait()
	return f.File.Sync()
}

func (f *controlSyncFile) SyncData() error {
	f.wait()
	return f.File.SyncData()
}

func TestRaftControlMetadataForcesActualWALSync(t *testing.T) {
	for _, withData := range []bool{false, true} {
		t.Run(fmt.Sprint(withData), func(t *testing.T) {
			dir := t.TempDir()
			ps := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
			require.NoError(t, ps.Close())
			fs := &controlSyncFS{FS: vfs.Default, entered: make(chan struct{}), release: make(chan struct{})}
			opts, cache := defaultPebbleOptionsWithCache(false)
			opts.FS = fs
			db, err := pebble.Open(dir, opts)
			require.NoError(t, err)
			ps.db, ps.cache = db, cache
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(fs.release) }) }
			t.Cleanup(func() {
				release()
				require.NoError(t, ps.Close())
			})
			fs.armed.Store(true)
			done := make(chan error, 1)
			go func() {
				done <- ps.ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
					if withData {
						if err := ps.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "value"), nil, 10, 10, 7); err != nil {
							return err
						}
					}
					return ps.StageRaftControlMetadata(ctx, []byte("R/7"), []byte("receipt"))
				})
			}()
			select {
			case <-fs.entered:
			case err := <-done:
				t.Fatalf("entry returned before actual WAL sync: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("entry never reached the actual WAL sync")
			}
			select {
			case err := <-done:
				t.Fatalf("entry returned while WAL sync was blocked: %v", err)
			default:
			}
			release()
			require.NoError(t, <-done)
			index, present := raftEntryAppliedIndex(t, ps)
			require.True(t, present)
			require.Equal(t, uint64(7), index)
		})
	}
}

type controlFailingNonce struct{ err error }

func (f controlFailingNonce) Next() ([encryption.NonceSize]byte, error) {
	return [encryption.NonceSize]byte{}, f.err
}

func TestRaftControlMetadataEncryptionFailureAbortsInnerData(t *testing.T) {
	fixture := newEncryptedStoreFixture(t, 1)
	ps := pebbleStoreApplied(t, fixture.mvcc)
	boom := errors.New("injected nonce failure")
	nonces := ps.nonceFactory
	err := ps.ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		require.NoError(t, ps.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "must-abort"), nil, 10, 10, 7))
		ps.nonceFactory = controlFailingNonce{err: boom}
		defer func() { ps.nonceFactory = nonces }()
		require.ErrorIs(t, ps.StageRaftControlMetadata(ctx, []byte("P"), []byte("must-abort")), boom)
		return nil
	})
	require.ErrorIs(t, err, boom)
	_, err = ps.GetAt(t.Context(), []byte("inner"), 100)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = ps.ReadRaftControlMetadata(t.Context(), []byte("P"))
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Zero(t, ps.LastCommitTS())
	_, present := raftEntryAppliedIndex(t, ps)
	require.False(t, present)
}

func TestRaftControlCrashHelper(t *testing.T) {
	if os.Getenv("ELASTICKV_CONTROL_CRASH_HELPER") != "1" {
		return
	}
	dir, err := os.Getwd()
	require.NoError(t, err)
	ps := newPebbleStoreWithFSMApplyWriteOptsForTest(t, dir, pebble.NoSync, fsmSyncModeNoSync)
	ready := func() {
		_, _ = fmt.Fprintln(os.Stdout, "READY")
		_, _ = io.Copy(io.Discard, os.Stdin)
	}
	require.NoError(t, ps.ApplyRaftEntry(t.Context(), 7, func(ctx context.Context) error {
		if err := ps.StageRaftControlMetadata(ctx, []byte("P"), []byte("durable-policy")); err != nil {
			return err
		}
		if err := ps.ApplyMutationsRaftAt(ctx, raftEntryPut("inner", "durable-data"), nil, 10, 10, 7); err != nil {
			return err
		}
		if os.Getenv("ELASTICKV_CONTROL_CRASH_PHASE") == "before-commit" {
			ready()
		}
		return nil
	}))
	ready()
}

// Process kill/reopen is not a power-loss simulation. The separate WAL-sync
// witness checks that successful control entries actually wait for fsync.
func TestRaftControlMetadataProcessKillReopen(t *testing.T) {
	for _, phase := range []string{"before-commit", "after-commit"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			executable, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRaftControlCrashHelper$")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "ELASTICKV_CONTROL_CRASH_HELPER=1", "ELASTICKV_CONTROL_CRASH_PHASE="+phase)
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			stdin, err := cmd.StdinPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			joined := false
			defer func() {
				_ = stdin.Close()
				if !joined {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			line, err := bufio.NewReader(stdout).ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "READY\n", line)
			require.NoError(t, cmd.Process.Kill())
			require.Error(t, cmd.Wait())
			joined = true
			reopened, err := NewPebbleStore(dir)
			require.NoError(t, err)
			defer reopened.Close()
			policy, controlErr := raftMetadata(t, reopened).ReadRaftControlMetadata(t.Context(), []byte("P"))
			data, dataErr := reopened.GetAt(t.Context(), []byte("inner"), 10)
			index, present := raftEntryAppliedIndex(t, reopened)
			if phase == "before-commit" {
				require.ErrorIs(t, controlErr, ErrKeyNotFound)
				require.ErrorIs(t, dataErr, ErrKeyNotFound)
				require.False(t, present)
				require.Zero(t, reopened.LastCommitTS())
			} else {
				require.NoError(t, controlErr)
				require.NoError(t, dataErr)
				require.Equal(t, []byte("durable-policy"), policy)
				require.Equal(t, []byte("durable-data"), data)
				require.True(t, present)
				require.Equal(t, uint64(7), index)
				require.Equal(t, uint64(10), reopened.LastCommitTS())
			}
		})
	}
}
