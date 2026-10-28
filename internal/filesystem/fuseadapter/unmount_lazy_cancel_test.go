package fuseadapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"

	"github.com/stretchr/testify/require"
)

// TestUnmountLazyCancelsInFlightRequests pins that the shutdown fallback
// stops serving: a handler blocked in a context-aware backend call (the
// reason the plain unmount hung or a client kept the mount busy) has its
// request context cancelled, so it returns before the shard stores and the
// Raft runtimes behind it are closed, and a request arriving afterwards
// starts already cancelled.
func TestUnmountLazyCancelsInFlightRequests(t *testing.T) {
	fs := NewRawFileSystem(nil)
	server := &Server{fs: fs}
	inFlight, done := fs.requestContext(nil)
	defer done()

	require.NoError(t, server.UnmountLazy(context.Background()))
	testutil.Closed(t, inFlight.Done(), 5*time.Second, "the in-flight request was not cancelled by the lazy detach")

	late, lateDone := fs.requestContext(nil)
	defer lateDone()
	require.Error(t, late.Err(), "a request arriving after the lazy detach must start cancelled")
}
