package fuseadapter_test

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/filesystem/fstest"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/stretchr/testify/require"
)

// TestMountContextAbortsWhenCancelled pins that a shutdown requested while
// the filesystem is still starting stops MountContext before any pre-mount
// work (stale-mount detach, intent recovery) instead of running it to
// completion. No kernel mount is attempted, so no FUSE is needed.
func TestMountContextAbortsWhenCancelled(t *testing.T) {
	svc, err := fstest.NewMemoryService()
	require.NoError(t, err)
	adapter := fuseadapter.New(svc, []byte("cancelled"))
	t.Cleanup(adapter.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	server, err := fuseadapter.MountContext(ctx, t.TempDir(), adapter, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "aborted before start")
	require.Nil(t, server)
}
