package adapter

import (
	"bytes"
	"context"
	"math/rand/v2"
	"strings"
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
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
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
		if !isRetryableRedisTxnErr(err) {
			return err
		}

		err = normalizeRetryableRedisTxnErr(err)
		nextPolicy := retryPolicyForRedisTxnErr(err)
		if attempt == 0 || nextPolicy != policy {
			backoff = nextPolicy.initialBackoff
			policy = nextPolicy
		}
		if attempt >= policy.maxAttempts {
			return errors.Wrap(err, "redis txn retry limit exceeded")
		}
		if !waitRedisRetryBackoff(ctx, backoff) {
			return err
		}
		backoff = nextRedisRetryBackoff(backoff, policy.maxBackoff)
	}
}

func normalizeRetryableRedisTxnErr(err error) error {
	key, ok := extractRetryableRedisTxnKey(err.Error())
	if !ok {
		return err
	}
	logicalKey := normalizeRetryableRedisTxnKey(key)
	if len(logicalKey) == 0 || bytes.Equal(logicalKey, key) {
		return err
	}

	switch {
	case errors.Is(err, kv.ErrTxnLocked):
		return errors.Wrapf(kv.ErrTxnLocked, "key: %s", string(logicalKey))
	case errors.Is(err, store.ErrWriteConflict):
		return errors.Wrapf(store.ErrWriteConflict, "key: %s", string(logicalKey))
	default:
		return err
	}
}

func extractRetryableRedisTxnKey(msg string) ([]byte, bool) {
	const marker = "key: "
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return nil, false
	}
	start := idx + len(marker)
	end := len(msg)
	for _, suffix := range []string{
		": txn locked",
		": write conflict",
		" (timestamp overflow)",
	} {
		if pos := strings.Index(msg[start:], suffix); pos >= 0 && start+pos < end {
			end = start + pos
		}
	}
	if start >= end {
		return nil, false
	}
	return []byte(msg[start:end]), true
}

func normalizeRetryableRedisTxnKey(key []byte) []byte {
	if userKey := kv.ExtractTxnUserKey(key); userKey != nil {
		key = userKey
	}
	if store.IsListMetaKey(key) || store.IsListItemKey(key) {
		return store.ExtractListUserKey(key)
	}
	if bytes.HasPrefix(key, []byte(redisTTLPrefix)) {
		return bytes.TrimPrefix(key, []byte(redisTTLPrefix))
	}
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		return userKey
	}
	return key
}
