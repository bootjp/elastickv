package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
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
