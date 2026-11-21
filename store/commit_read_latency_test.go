package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

type pausedSyncFS struct {
	vfs.FS
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}
type pausedSyncFile struct {
	vfs.File
	fs *pausedSyncFS
}

func (f *pausedSyncFile) pause() {
	if f.fs.armed.Load() {
		f.fs.once.Do(func() { close(f.fs.entered) })
		<-f.fs.release
	}
}
func (f *pausedSyncFile) Sync() error                  { f.pause(); return f.File.Sync() }
func (f *pausedSyncFile) SyncData() error              { f.pause(); return f.File.SyncData() }
func (f *pausedSyncFile) SyncTo(n int64) (bool, error) { f.pause(); return f.File.SyncTo(n) }
func (f *pausedSyncFS) Create(name string, cat vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, cat)
	if err != nil {
		return nil, err
	}
	return &pausedSyncFile{file, f}, nil
}
func TestCommittedReadDoesNotWaitForUnrelatedWALSync(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "entry"}[scoped], func(t *testing.T) {
			fs := &pausedSyncFS{FS: vfs.NewMem(), entered: make(chan struct{}), release: make(chan struct{})}
			db, err := pebble.Open("db", &pebble.Options{FS: fs})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			s := &pebbleStore{db: db, fsmApplyWriteOpts: pebble.Sync, writeConflicts: newWriteConflictCounter()}
			require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("old", "value"), nil, 1, 1, 1))
			fs.armed.Store(true)
			applyDone := make(chan error, 1)
			go func() {
				apply := func(ctx context.Context) error {
					return s.ApplyMutationsRaftAt(ctx, raftEntryPut("new", "value"), nil, 2, 2, 2)
				}
				if scoped {
					applyDone <- s.ApplyRaftEntry(t.Context(), 2, apply)
				} else {
					applyDone <- apply(t.Context())
				}
			}()
			select {
			case <-fs.entered:
			case <-time.After(time.Second):
				close(fs.release)
				t.Fatal("WAL sync was not reached")
			}
			readDone := make(chan error, 1)
			go func() { _, e := s.GetAt(t.Context(), []byte("old"), 1); readDone <- e }()
			blocked := false
			select {
			case e := <-readDone:
				require.NoError(t, e)
			case <-time.After(100 * time.Millisecond):
				blocked = true
				t.Error("committed historical read blocked behind unrelated WAL fsync")
			}
			close(fs.release)
			if blocked {
				require.NoError(t, <-readDone)
			}
			require.NoError(t, <-applyDone)
			require.Equal(t, uint64(2), s.LastCommitTS())
		})
	}
}
func TestCommittedWatermarkDoesNotWaitForUnrelatedWALSync(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "entry"}[scoped], func(t *testing.T) {
			fs := &pausedSyncFS{FS: vfs.NewMem(), entered: make(chan struct{}), release: make(chan struct{})}
			db, err := pebble.Open("db", &pebble.Options{FS: fs})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			s := &pebbleStore{db: db, fsmApplyWriteOpts: pebble.Sync, writeConflicts: newWriteConflictCounter()}
			require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("old", "value"), nil, 1, 1, 1))
			fs.armed.Store(true)
			applyDone := make(chan error, 1)
			go func() {
				apply := func(ctx context.Context) error {
					return s.ApplyMutationsRaftAt(ctx, raftEntryPut("new", "value"), nil, 2, 2, 2)
				}
				if scoped {
					applyDone <- s.ApplyRaftEntry(t.Context(), 2, apply)
				} else {
					applyDone <- apply(t.Context())
				}
			}()
			select {
			case <-fs.entered:
			case <-time.After(time.Second):
				close(fs.release)
				t.Fatal("WAL sync was not reached")
			}
			readDone := make(chan error, 1)
			go func() {
				ts := s.LastCommitTS()
				if ts != 1 {
					readDone <- fmt.Errorf("watermark advanced before sync: %d", ts)
					return
				}
				readDone <- nil
			}()
			blocked := false
			select {
			case e := <-readDone:
				require.NoError(t, e)
			case <-time.After(100 * time.Millisecond):
				blocked = true
				t.Error("committed watermark blocked behind unrelated WAL fsync")
			}
			close(fs.release)
			if blocked {
				require.NoError(t, <-readDone)
			}
			require.NoError(t, <-applyDone)
			require.Equal(t, uint64(2), s.LastCommitTS())
		})
	}
}

func TestHistoricalReadWaitsForLowTimestampWALSync(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, commitTS := range []uint64{5, 7, 10} {
			t.Run(fmt.Sprintf("entry=%t/ts=%d", scoped, commitTS), func(t *testing.T) {
				s, fs, release := newRaftDataWALSyncStore(t)
				require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("key", "old"), nil, 5, 5, 1))
				require.NoError(t, s.ApplyMutationsRaftAt(t.Context(), raftEntryPut("seed", "v"), nil, 10, 10, 2))
				fs.armed.Store(true)
				done := make(chan error, 1)
				go func() {
					apply := func(ctx context.Context) error {
						return s.ApplyMutationsRaftAt(ctx, raftEntryPut("key", "new"), nil, commitTS, commitTS, 3)
					}
					if scoped {
						done <- s.ApplyRaftEntry(t.Context(), 3, apply)
					} else {
						done <- apply(t.Context())
					}
				}()
				select {
				case <-fs.entered:
				case err := <-done:
					t.Fatalf("apply did not wait for WAL sync: %v", err)
				case <-time.After(time.Second):
					t.Fatal("WAL sync was not reached")
				}
				read := make(chan error, 1)
				go func() { _, err := s.GetAt(t.Context(), []byte("key"), 10); read <- err }()
				early := false
				select {
				case <-read:
					early = true
					t.Error("historical read bypassed the low-timestamp publication guard")
				case <-time.After(100 * time.Millisecond):
				}
				release()
				require.NoError(t, <-done)
				if !early {
					require.NoError(t, <-read)
				}
				value, err := s.GetAt(t.Context(), []byte("key"), 10)
				require.NoError(t, err)
				require.Equal(t, []byte("new"), value)
			})
		}
	}
}
