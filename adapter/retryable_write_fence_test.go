package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRedisRetriesOnlyTypedOnePhaseFence(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := (&RedisServer{}).retryRedisWrite(context.Background(), func() error {
		attempts++
		if attempts == 1 {
			return errors.WithStack(kv.ErrRouteWriteFenced)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)

	for _, unproven := range []error{
		&kv.ForwardFailure{Decision: pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN,
			Reason: pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE},
		kv.ForwardDecisionStatus(pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY,
			pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE,
			pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED),
	} {
		attempts = 0
		err := (&RedisServer{}).retryRedisWrite(context.Background(), func() error {
			attempts++
			return unproven
		})
		require.Equal(t, 1, attempts)
		require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
		require.False(t, isRouteWriteFencedError(unproven))
	}
}

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

func TestUnprovenWireWriteFenceErrorsAreNotAdapterRetryable(t *testing.T) {
	t.Parallel()

	for _, code := range []codes.Code{codes.Unknown, codes.Aborted, codes.FailedPrecondition} {
		err := errors.WithStack(status.Error(
			code,
			"commit-version v=12: key \"k\" routeKey \"k\": "+kv.ErrRouteWriteFenced.Error(),
		))
		require.False(t, isRetryableRedisTxnErr(err))
		require.False(t, isRetryableS3MutationErr(err))
		require.False(t, isRetryableTransactWriteError(err))
		require.False(t, isRouteWriteFencedError(err))
		require.False(t, isIgnorableTransactRaceError(err))
	}
}

func TestUntrustedStatusCannotBorrowLocalConflictForRetry(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{store.ErrWriteConflict, kv.ErrTxnLocked, kv.ErrRouteWriteFenced} {
		err := errors.Join(cause, kv.ForwardWriteConflictStatus())
		require.False(t, isRetryableTransactWriteError(err))
		require.False(t, isIgnorableTransactRaceError(err))
		require.False(t, isRetryableRedisTxnErr(err))
		require.False(t, isRetryableS3MutationErr(err))
		require.False(t, isRouteWriteFencedError(err))
		require.False(t, isAmbiguousRedisExecDispatchErr(err))
	}
}

func TestWireWriteFenceMatcherRequiresSentinelSuffix(t *testing.T) {
	t.Parallel()

	err := status.Error(codes.Unknown, kv.ErrRouteWriteFenced.Error()+": "+store.ErrWriteConflict.Error())

	require.False(t, isRouteWriteFencedError(err))
	require.False(t, isRetryableTransactWriteError(err))
}

func TestUnknownOutcomeNeverBecomesAdapterRetryOrIgnorableRace(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{
		store.ErrWriteConflict,
		kv.ErrTxnLocked,
		kv.ErrRouteWriteFenced,
		kv.ErrComposed1Violation,
	} {
		err := errors.Join(kv.ErrTransactionOutcomeUnknown, cause)
		require.False(t, isRetryableRedisTxnErr(err))
		require.False(t, isReusableRedisTxnErr(err))
		require.False(t, isRedisComposedRouteErr(err))
		require.ErrorIs(t, normalizeRetryableRedisTxnErr(err), kv.ErrTransactionOutcomeUnknown)
		require.False(t, isRetryableS3MutationErr(err))
		require.False(t, isRetryableTransactWriteError(err))
		require.False(t, isIgnorableTransactRaceError(err))
		require.False(t, isRouteWriteFencedError(err))
		require.False(t, isAmbiguousRedisExecDispatchErr(err))
		require.False(t, splitJobPhaseErrorRetryable(err))
		require.Equal(t, codes.Unknown, status.Code(splitPartitionCoordinatorStatusError(err)))
		require.Equal(t, codes.Unknown, status.Code(splitJobCoordinatorStatusError(err)))
		require.Equal(t, codes.Unknown, status.Code(splitJobPromotionStatusError(err)))
		require.Contains(t, status.Convert(splitJobCoordinatorStatusError(err)).Message(), kv.ErrTransactionOutcomeUnknown.Error())
		repairRetry, repairErr := normalizeS3ChunkBlobRepairError(err)
		require.False(t, repairRetry)
		require.Equal(t, codes.Unknown, status.Code(repairErr))
		calls := 0
		retryErr := (&RedisServer{}).retryRedisWrite(t.Context(), func() error {
			calls++
			return err
		})
		require.ErrorIs(t, retryErr, kv.ErrTransactionOutcomeUnknown)
		require.Equal(t, 1, calls)
	}
}
