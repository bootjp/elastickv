//go:build linux

package fuseadapter_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

// TestMountContextAbortsWhileMountHelperHangs puts a fusermount3 stand-in
// that never answers on PATH and checks that cancelling the context unwinds
// MountContext while go-fuse is still waiting on the helper. go-fuse itself
// waits on the helper with no context, so this is the only way a shutdown
// signal during the kernel mount can get out.
func TestMountContextAbortsWhileMountHelperHangs(t *testing.T) {
	helperDir := t.TempDir()
	// The stand-in holds go-fuse's communication socket open and never sends
	// a descriptor; it exits on its own so no process outlives the test by
	// much.
	helper := filepath.Join(helperDir, "fusermount3")
	require.NoError(t, os.WriteFile(helper, []byte("#!/bin/sh\nexec /bin/sleep 3\n"), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	t.Setenv("PATH", helperDir)

	svc, err := fstest.NewMemoryService()
	require.NoError(t, err)
	adapter := fuseadapter.New(svc, []byte("hung-helper"))
	t.Cleanup(adapter.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	// DirectMount off: force the fusermount helper path even when root.
	server, err := fuseadapter.MountContext(ctx, t.TempDir(), adapter, &fuse.MountOptions{DirectMount: false})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "waiting for the mount helper")
	require.Nil(t, server)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 150*time.Millisecond, "the helper must really have been waited on until the deadline")
	require.Less(t, elapsed, 2*time.Second, "MountContext must return on cancellation, not when the helper exits")
}
