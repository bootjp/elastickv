package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteFenceErrorsAreAdapterRetryable(t *testing.T) {
	t.Parallel()

	require.True(t, isRetryableRedisTxnErr(kv.ErrRouteWriteFenced))
	require.True(t, isRetryableS3MutationErr(kv.ErrRouteWriteFenced))
	require.True(t, isRetryableTransactWriteError(kv.ErrRouteWriteFenced))
	require.False(t, shouldPreserveTransactWriteAttempt(kv.ErrRouteWriteFenced))
	require.False(t, isIgnorableTransactRaceError(kv.ErrRouteWriteFenced))
	require.True(t, isIgnorableTransactRaceError(store.ErrWriteConflict))
	require.True(t, isIgnorableTransactRaceError(kv.ErrTxnLocked))
}

func TestWireWriteFenceErrorsAreAdapterRetryable(t *testing.T) {
	t.Parallel()

	err := errors.WithStack(status.Error(
		codes.Unknown,
		"commit-version v=12: key \"k\" routeKey \"k\": "+kv.ErrRouteWriteFenced.Error(),
	))

	require.True(t, isRetryableRedisTxnErr(err))
	require.True(t, isRetryableS3MutationErr(err))
	require.True(t, isRetryableTransactWriteError(err))
	require.False(t, shouldPreserveRedisTxnAttempt(err))
	require.False(t, shouldPreserveTransactWriteAttempt(err))
	require.False(t, isIgnorableTransactRaceError(err))
}

func TestWireWriteFenceMatcherRequiresSentinelSuffix(t *testing.T) {
	t.Parallel()

	err := status.Error(codes.Unknown, kv.ErrRouteWriteFenced.Error()+": "+store.ErrWriteConflict.Error())

	require.False(t, isRouteWriteFencedError(err))
	require.False(t, isRetryableTransactWriteError(err))
}
