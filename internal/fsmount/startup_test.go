package fsmount

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestRetryFilesystemStartupWaitsForLeadershipAndHLCLease(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := retryFilesystemStartup(t.Context(), time.Nanosecond, func(context.Context) error {
		attempts++
		switch attempts {
		case 1:
			return kv.ErrLeaderNotFound
		case 2:
			return errors.Join(kv.ErrCeilingExpired, errors.New("lease recovery unavailable"))
		default:
			return nil
		}
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryFilesystemStartupRejectsNonTransientError(t *testing.T) {
	t.Parallel()
	permanent := errors.New("corrupt filesystem root")
	attempts := 0
	err := retryFilesystemStartup(t.Context(), time.Nanosecond, func(context.Context) error {
		attempts++
		return permanent
	})
	require.ErrorIs(t, err, permanent)
	require.Equal(t, 1, attempts)
}

func TestRetryFilesystemStartupStopsAtDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	err := retryFilesystemStartup(ctx, time.Millisecond, func(context.Context) error {
		return kv.ErrLeaderNotFound
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, kv.ErrLeaderNotFound)
}
