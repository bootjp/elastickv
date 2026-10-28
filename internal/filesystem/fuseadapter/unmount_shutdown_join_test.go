package fuseadapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryUnmountCancelsRequestsAfterMountRemoved(t *testing.T) {
	raw := NewRawFileSystem(nil)
	request, cancel := raw.requestContext(nil)
	defer cancel()
	useFakeDetachMountTable(t, "")
	server := &Server{inner: &fuse.Server{}, fs: raw, mountPoint: t.TempDir(), fsType: fuseTypeFor(filesystemName)}
	require.NoError(t, server.UnmountContext(t.Context()))
	require.ErrorIs(t, request.Err(), context.Canceled, "successful unmount must cancel handlers even before Serve returns")
}

func TestJoinAbandonedMountWaitsForCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		done <- joinAbandonedMount(func() { close(entered); <-release }, time.Second)
	}()
	<-entered
	testutil.NoRecv(t, done, time.Millisecond, "startup returned before its late mount cleanup")
	close(release)
	require.True(t, <-done)
}

func TestJoinAbandonedMountHasBound(t *testing.T) {
	release, finished := make(chan struct{}), make(chan struct{})
	require.False(t, joinAbandonedMount(func() { <-release; close(finished) }, time.Millisecond))
	close(release)
	<-finished
}
