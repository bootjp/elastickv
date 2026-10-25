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
	return isReusableRedisTxnErr(err) || isRedisComposedRouteErr(err)
}

func isReusableRedisTxnErr(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) ||
		errors.Is(err, kv.ErrTxnLocked) ||
		wireRedisTxnErrKind(err) == redisTxnWireErrLocked ||
		isRouteWriteFencedError(err)
}

func shouldPreserveRedisTxnAttempt(err error) bool {
	return isRetryableRedisTxnErr(err) &&
		!isRouteWriteFencedError(err) &&
		!isRedisComposedRouteErr(err)
}

func isRedisComposedRouteErr(err error) bool {
	if errors.Is(err, kv.ErrComposed1Violation) ||
		errors.Is(err, kv.ErrComposed1VersionGCd) {
		return true
	}
	parsed := parseWireRedisTxnErr(err)
	return errors.Is(parsed, kv.ErrComposed1Violation) ||
		errors.Is(parsed, kv.ErrComposed1VersionGCd)
}

func retryPolicyForRedisTxnErr(err error) redisTxnRetryPolicy {
	if errors.Is(err, kv.ErrTxnLocked) || wireRedisTxnErrKind(err) == redisTxnWireErrLocked {
		return redisTxnLockedRetryPolicy
	}
	return redisWriteConflictRetryPolicy
}

type redisTxnWireErrKind uint8

const (
	redisTxnWireErrNone redisTxnWireErrKind = iota
	redisTxnWireErrWriteConflict
	redisTxnWireErrLocked
)

// parseWireRedisTxnErr restores transaction error typing after an internal
// leader redirect crosses gRPC. Structured status details preserve the
// rejection kind without interpreting diagnostic messages or transporting keys.
// Wire write conflicts are not generally retryable: a lost forwarding response
// can turn an already-applied write into a later self-conflict. Reuse-aware
// callers explicitly normalize them before retrying; all other callers return
// the normalized error without replaying the operation or exposing the internal
// key layout.
func parseWireRedisTxnErr(err error) error {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return nil
	}
	if kv.IsForwardWriteConflict(err) {
		return errors.WithStack(store.ErrWriteConflict)
	}
	if kv.IsForwardTxnLocked(err) {
		return errors.WithStack(kv.ErrTxnLocked)
	}
	if kv.IsForwardComposed1Violation(err) {
		return errors.WithStack(kv.ErrComposed1Violation)
	}
	if kv.IsForwardComposed1VersionGCd(err) {
		return errors.WithStack(kv.ErrComposed1VersionGCd)
	}
	return nil
}

func wireRedisTxnErrKind(err error) redisTxnWireErrKind {
	parsed := parseWireRedisTxnErr(err)
	switch {
	case errors.Is(parsed, store.ErrWriteConflict):
		return redisTxnWireErrWriteConflict
	case errors.Is(parsed, kv.ErrTxnLocked):
		return redisTxnWireErrLocked
	default:
		return redisTxnWireErrNone
	}
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
		// Classify the original transport shape before normalization. A wire
		// write conflict is ambiguous — the forwarded apply may already have
		// landed — so the generic loop must return it without replaying. Wire
		// TxnLocked remains retryable because the lock rejection did not apply
		// the transaction. Reuse-aware callers convert wire conflicts to typed
		// errors before returning them here.
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
	if parsed := parseWireRedisTxnErr(err); parsed != nil {
		return parsed
	}
	if errors.Is(err, kv.ErrTxnLocked) {
		return errors.WithStack(kv.ErrTxnLocked)
	}
	if errors.Is(err, store.ErrWriteConflict) {
		return errors.WithStack(store.ErrWriteConflict)
	}
	return err
}
