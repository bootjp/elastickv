package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/stretchr/testify/require"
)

type testFilesystemLeaseReaper struct {
	cancel context.CancelFunc
	calls  int
	limits []int
}

func (r *testFilesystemLeaseReaper) ReapExpiredOpenHandleLeases(
	_ context.Context,
	limit int,
) (filesystem.LeaseReapStats, error) {
	r.calls++
	r.limits = append(r.limits, limit)
	if r.calls == 2 {
		r.cancel()
	}
	return filesystem.LeaseReapStats{}, nil
}

func TestRunFilesystemLeaseReaperRunsPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reaper := &testFilesystemLeaseReaper{cancel: cancel}

	require.NoError(t, runFilesystemLeaseReaper(ctx, reaper, time.Millisecond))
	require.Equal(t, 2, reaper.calls)
	require.Equal(t, []int{0, 0}, reaper.limits)
}
