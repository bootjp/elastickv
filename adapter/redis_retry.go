package adapter

import (
	"bytes"
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
	if key, detail, ok := kv.TxnLockedDetails(err); ok {
		logicalKey := normalizeRetryableRedisTxnKey(key)
		if len(logicalKey) == 0 || bytes.Equal(logicalKey, key) {
			return err
		}
		if detail != "" {
			return errors.WithStack(kv.NewTxnLockedErrorWithDetail(logicalKey, detail))
		}
		return errors.WithStack(kv.NewTxnLockedError(logicalKey))
	}
	if key, ok := store.WriteConflictKey(err); ok {
		logicalKey := normalizeRetryableRedisTxnKey(key)
		if len(logicalKey) == 0 || bytes.Equal(logicalKey, key) {
			return err
		}
		return errors.WithStack(store.NewWriteConflictError(logicalKey))
	}
	return err
}

// normalizeWideColumnKey extracts the logical user key from any wide-column
// internal key (hash/set meta, delta, or field/member). Returns (key, true) when
// the input is a recognised wide-column key, (nil, false) otherwise.
// Delta prefixes are checked before meta prefixes because delta keys share the
// meta prefix as a leading substring.
func normalizeWideColumnKey(key []byte) ([]byte, bool) {
	if store.IsHashMetaDeltaKey(key) {
		return store.ExtractHashUserKeyFromDelta(key), true
	}
	if store.IsHashMetaKey(key) {
		return store.ExtractHashUserKeyFromMeta(key), true
	}
	if store.IsHashFieldKey(key) {
		return store.ExtractHashUserKeyFromField(key), true
	}
	if store.IsSetMetaDeltaKey(key) {
		return store.ExtractSetUserKeyFromDelta(key), true
	}
	if store.IsSetMetaKey(key) {
		return store.ExtractSetUserKeyFromMeta(key), true
	}
	if store.IsSetMemberKey(key) {
		return store.ExtractSetUserKeyFromMember(key), true
	}
	return nil, false
}

func normalizeRetryableRedisTxnKey(key []byte) []byte {
	if userKey := kv.ExtractTxnUserKey(key); userKey != nil {
		key = userKey
	}
	if store.IsListMetaKey(key) || store.IsListItemKey(key) {
		return store.ExtractListUserKey(key)
	}
	if wideKey, ok := normalizeWideColumnKey(key); ok {
		return wideKey
	}
	if bytes.HasPrefix(key, []byte(redisTTLPrefix)) {
		return bytes.TrimPrefix(key, []byte(redisTTLPrefix))
	}
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		return userKey
	}
	return key
}
