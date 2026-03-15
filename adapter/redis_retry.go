package adapter

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	redisTxnRetryInitialBackoff = 1 * time.Millisecond
	redisTxnRetryMaxBackoff     = 10 * time.Millisecond
	redisTxnRetryBackoffFactor  = 2
	redisTxnRetryMaxAttempts    = 50
)

func isRetryableRedisTxnErr(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
}

func waitRedisRetryBackoff(ctx context.Context, delay time.Duration) bool {
	half := delay / redisTxnRetryBackoffFactor
	jittered := delay + time.Duration(rand.Int64N(int64(half))) //nolint:gosec // jitter for retry backoff, not security-sensitive
	timer := time.NewTimer(jittered)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextRedisRetryBackoff(current time.Duration) time.Duration {
	next := current * redisTxnRetryBackoffFactor
	if next > redisTxnRetryMaxBackoff {
		return redisTxnRetryMaxBackoff
	}
	return next
}

func (r *RedisServer) retryRedisWrite(ctx context.Context, fn func() error) error {
	backoff := redisTxnRetryInitialBackoff
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryableRedisTxnErr(err) {
			return err
		}
		if attempt >= redisTxnRetryMaxAttempts {
			return errors.Wrap(err, "redis txn retry limit exceeded")
		}
		if !waitRedisRetryBackoff(ctx, backoff) {
			return err
		}
		backoff = nextRedisRetryBackoff(backoff)
	}
}
