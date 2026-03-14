package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	redisTxnRetryInitialBackoff = 1 * time.Millisecond
	redisTxnRetryMaxBackoff     = 10 * time.Millisecond
	redisTxnRetryBackoffFactor  = 2
)

func isRetryableRedisTxnErr(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
}

func waitRedisRetryBackoff(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
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
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryableRedisTxnErr(err) {
			return err
		}
		if !waitRedisRetryBackoff(ctx, backoff) {
			return err
		}
		backoff = nextRedisRetryBackoff(backoff)
	}
}
