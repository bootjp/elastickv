package fuseadapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestDelayedMountHelperLookupKeepsFallbackSnapshot(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	oldDir := t.TempDir()
	want := writeFakeMountHelper(t, oldDir, "fusermount3")
	newDir := t.TempDir()
	writeFakeMountHelper(t, newDir, "fusermount3")
	previous := mountHelperFallbackDir
	mountHelperFallbackDir = oldDir
	t.Cleanup(func() { mountHelperFallbackDir = previous })

	lookup := snapshotMountHelperLookup()
	release := make(chan struct{})
	type result struct {
		path string
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		<-release
		path, err := lookup("fusermount3")
		finished <- result{path: path, err: err}
	}()
	// A caller can time out before its lookup runs. The next test/config
	// owner must not change that lookup's already-admitted dependencies.
	mountHelperFallbackDir = newDir
	close(release)
	got := <-finished
	require.NoError(t, got.err)
	require.Equal(t, want, got.path)
}

func TestCanceledMountHelperLookupDoesNotLaunchWorker(t *testing.T) {
	previous := lookupMountHelperFn
	called := make(chan struct{}, 1)
	lookupMountHelperFn = func(string) (string, error) {
		called <- struct{}{}
		return "", nil
	}
	t.Cleanup(func() { lookupMountHelperFn = previous })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := lookupMountHelperContext(ctx, "fusermount3")
	require.ErrorIs(t, err, context.Canceled)
	testutil.NoRecv(t, called, 10*time.Millisecond, "already-cancelled lookup launched background work")
}

func TestMountHelperLookupSnapshotSurvivesConcurrentOwner(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	previous := mountHelperFallbackDir
	mountHelperFallbackDir = t.TempDir()
	nextDir := t.TempDir()
	t.Cleanup(func() { mountHelperFallbackDir = previous })
	lookup := snapshotMountHelperLookup()
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		close(started)
		_, err := lookup("fusermount3")
		finished <- err
	}()
	<-started
	// No release handshake orders this mutation before or after the worker's
	// lookup. A lookup that still reads the global races under -race.
	mountHelperFallbackDir = nextDir
	require.Error(t, <-finished)
}
