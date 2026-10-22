package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunFilesystemLeaseReaperSkipsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reaper := &testFilesystemLeaseReaper{cancel: cancel}

	require.NoError(t, runFilesystemLeaseReaper(ctx, reaper, time.Nanosecond))
	require.Zero(t, reaper.calls)
}

func TestRunFilesystemLeaseReaperCancellationWinsOverReadyTick(t *testing.T) {
	for range 50 {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		reaper := &testFilesystemLeaseReaper{cancel: cancel}

		require.NoError(t, runFilesystemLeaseReaper(ctx, reaper, time.Nanosecond))
		require.Equal(t, 2, reaper.calls, "a ready tick must not start another reap after cancellation")
	}
}
