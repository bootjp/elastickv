package adapter

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	redisTxnRetryInitialBackoff = 1 * time.Millisecond
	redisTxnRetryMaxBackoff     = 10 * time.Millisecond
	redisTxnLockInitialBackoff  = 5 * time.Millisecond
	redisTxnLockMaxBackoff      = 50 * time.Millisecond
	redisTxnRetryBackoffFactor  = 2
	redisTxnRetryMaxAttempts    = 50
)

type redisTxnRetryPolicy struct {
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxAttempts    int
}

var (
	redisWriteConflictRetryPolicy = redisTxnRetryPolicy{
		initialBackoff: redisTxnRetryInitialBackoff,
		maxBackoff:     redisTxnRetryMaxBackoff,
		maxAttempts:    redisTxnRetryMaxAttempts,
	}
	redisTxnLockedRetryPolicy = redisTxnRetryPolicy{
		initialBackoff: redisTxnLockInitialBackoff,
		maxBackoff:     redisTxnLockMaxBackoff,
		maxAttempts:    redisTxnRetryMaxAttempts,
	}
)

func isRetryableRedisTxnErr(err error) bool {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) || isRedisTransportUncertain(err) {
		return false
	}
	return isReusableRedisTxnErr(err) || isRedisComposedRouteErr(err)
}

func isReusableRedisTxnErr(err error) bool {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) || isRedisTransportUncertain(err) {
		return false
	}
	return errors.Is(err, store.ErrWriteConflict) ||
		errors.Is(err, kv.ErrTxnLocked) ||
		isRouteWriteFencedError(err)
}

func shouldPreserveRedisTxnAttempt(err error) bool {
	return isRetryableRedisTxnErr(err) &&
		!isRouteWriteFencedError(err) &&
		!isRedisComposedRouteErr(err) &&
		(!errors.Is(err, store.ErrWriteConflict) || !kv.IsCertifiedOnePhaseConflict(err))
}

func isRedisComposedRouteErr(err error) bool {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) || isRedisTransportUncertain(err) {
		return false
	}
	if errors.Is(err, kv.ErrComposed1Violation) ||
		errors.Is(err, kv.ErrComposed1VersionGCd) {
		return true
	}
	return false
}

func retryPolicyForRedisTxnErr(err error) redisTxnRetryPolicy {
	if errors.Is(err, kv.ErrTxnLocked) {
		return redisTxnLockedRetryPolicy
	}
	return redisWriteConflictRetryPolicy
}

func waitRedisRetryBackoff(ctx context.Context, delay time.Duration) bool {
	half := delay / redisTxnRetryBackoffFactor
	jittered := delay
	if half > 0 {
		jittered += time.Duration(rand.Int64N(int64(half))) //nolint:gosec // jitter for retry backoff, not security-sensitive
	}
	timer := time.NewTimer(jittered)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextRedisRetryBackoff(current time.Duration, maxBackoff time.Duration) time.Duration {
	next := current * redisTxnRetryBackoffFactor
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func (r *RedisServer) retryRedisWrite(ctx context.Context, fn func() error) error {
	backoff := redisWriteConflictRetryPolicy.initialBackoff
	policy := redisWriteConflictRetryPolicy
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		// Only typed local or authenticated Forward rejections are retryable.
		// A raw wire status does not prove that the operation did not commit.
		retryable := isRetryableRedisTxnErr(err)
		err = normalizeRetryableRedisTxnErr(err)
		if !retryable {
			return err
		}
		nextPolicy := retryPolicyForRedisTxnErr(err)
		if attempt == 0 || nextPolicy != policy {
			backoff = nextPolicy.initialBackoff
			policy = nextPolicy
		}
		if attempt >= policy.maxAttempts {
			return errors.Wrap(err, "redis txn retry limit exceeded")
		}
		slog.Default().Debug("retryRedisWrite: retrying on write conflict",
			"attempt", attempt+1, "backoff", backoff, "err", err)
		if !waitRedisRetryBackoff(ctx, backoff) {
			return err
		}
		backoff = nextRedisRetryBackoff(backoff, policy.maxBackoff)
	}
}

func normalizeRetryableRedisTxnErr(err error) error {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return err
	}
	if isRedisTransportUncertain(err) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	if errors.Is(err, kv.ErrTxnLocked) {
		return errors.WithStack(kv.ErrTxnLocked)
	}
	if errors.Is(err, store.ErrWriteConflict) {
		if kv.IsCertifiedOnePhaseConflict(err) {
			return &redisCertifiedConflict{cause: err}
		}
		return errors.WithStack(store.ErrWriteConflict)
	}
	return err
}

// Keep the apply proof for retry policy without exposing the underlying key
// or storage diagnostic as a Redis protocol error.
type redisCertifiedConflict struct{ cause error }

func (e *redisCertifiedConflict) Error() string { return store.ErrWriteConflict.Error() }

func (e *redisCertifiedConflict) Unwrap() error { return e.cause }
