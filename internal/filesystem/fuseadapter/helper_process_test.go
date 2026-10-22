//go:build linux || darwin || freebsd

package fuseadapter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnmountHelperCancellationKillsLateDescendant(t *testing.T) {
	dir := t.TempDir()
	helper, ready, late := filepath.Join(dir, "helper"), filepath.Join(dir, "ready"), filepath.Join(dir, "late")
	script := fmt.Sprintf("#!/bin/sh\n( /bin/sleep 1; printf late > '%s' ) &\nprintf ready > '%s'\nwait\n", late, ready)
	require.NoError(t, os.WriteFile(helper, []byte(script), 0o700)) //nolint:gosec // Executable helper fixture, isolated in the test's temporary directory.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runUnmountHelper(ctx, helper, dir, false) }()
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("unmount helper was not reaped after cancellation")
	}
	time.Sleep(1100 * time.Millisecond)
	_, err := os.Stat(late)
	require.ErrorIs(t, err, os.ErrNotExist, "an old helper descendant must not act after a replacement mount can appear")
}
