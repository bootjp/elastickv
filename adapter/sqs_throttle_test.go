package adapter

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBucketStore_DefaultOff_ShortCircuit pins the contract that a
// nil throttle config never allocates a bucket and never rejects.
// This is the hot path for unconfigured queues — every nil-check that
// short-circuits keeps the per-request cost at one map-load on the
// SQSServer struct and one nil-comparison in charge().
func TestBucketStore_DefaultOff_ShortCircuit(t *testing.T) {
	t.Parallel()
	store := newBucketStoreDefault()
	for range 100 {
		out := store.charge(nil, "orders", bucketActionSend, 1)
		require.True(t, out.allowed)
		require.False(t, out.bucketPresent, "nil cfg must not allocate a bucket")
	}
}

// TestBucketStore_Empty_ShortCircuit covers the post-validator
// canonicalisation path: an all-zero sqsQueueThrottle is equivalent
// to nil. Without this branch, a queue whose operator wrote
// "ThrottleSendCapacity=0" would still pay the bucket allocation.
func TestBucketStore_Empty_ShortCircuit(t *testing.T) {
	t.Parallel()
	store := newBucketStoreDefault()
	out := store.charge(&sqsQueueThrottle{}, "orders", bucketActionSend, 1)
	require.True(t, out.allowed)
	require.False(t, out.bucketPresent)
}

// TestBucketStore_FreshAllowsUpToCapacity checks the fresh-bucket
// initial-state contract: a brand-new bucket starts at full capacity
// and accepts exactly that many tokens before rejecting the next one.
// This matches both the AWS rate-limiter behaviour and the §3.1
// failover semantic ("fresh bucket on failover starts at capacity").
func TestBucketStore_FreshAllowsUpToCapacity(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	for i := range 10 {
		out := store.charge(cfg, "orders", bucketActionSend, 1)
		require.True(t, out.allowed, "send %d must be allowed", i+1)
	}
	out := store.charge(cfg, "orders", bucketActionSend, 1)
	require.False(t, out.allowed, "11th send must be rejected")
	require.Equal(t, time.Second, out.retryAfter, "Retry-After floor is 1s")
}

// TestBucketStore_RefillBetweenChargesUsesElapsed pins the refill
// math: tokens accrue at refillRate per elapsed second, capped at
// capacity. Time is injected so the test does not race the wall
// clock.
func TestBucketStore_RefillBetweenChargesUsesElapsed(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 5}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	// Drain.
	for range 10 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	}
	require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	// Advance 1.5s → 7.5 tokens accrued (capped under capacity 10).
	now = now.Add(1500 * time.Millisecond)
	for range 7 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed,
			"after 1.5s refill at 5 RPS, 7 sends must succeed")
	}
	// 8th must reject — only 7.5 tokens accrued, charged 7, leaves 0.5.
	require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
}

// TestBucketStore_RefillCapsAtCapacity pins the upper bound on
// long-idle refill: a queue idle for an hour does NOT come back with
// 3600 tokens — the bucket caps at the configured capacity.
func TestBucketStore_RefillCapsAtCapacity(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, 2*time.Hour)
	require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	now = now.Add(time.Hour) // 3600 seconds, would be 3600 tokens uncapped
	for range 10 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	}
	require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed,
		"refill capped at capacity: 11th send post-idle must reject")
}

// TestBucketStore_BatchRejectsWholeBatchWhenShort pins the §3.3
// "batch verbs charge before dispatching individual entries" rule.
// A bucket with 3 tokens facing a 10-entry batch rejects the whole
// call and consumes nothing — partial-credit behaviour would make the
// "I have 3, you wanted 10" semantics ambiguous and AWS itself
// rejects the whole call.
func TestBucketStore_BatchRejectsWholeBatchWhenShort(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	// Drain to 3.
	for range 7 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	}
	// Try a 10-entry batch — should reject without consuming the 3.
	out := store.charge(cfg, "orders", bucketActionSend, 10)
	require.False(t, out.allowed)
	require.Equal(t, 7*time.Second, out.retryAfter,
		"Retry-After computed from (10-3)/1 = 7s")
	// The 3 leftover tokens are still spendable.
	for range 3 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed,
			"the rejected batch must not have drained the leftover credit")
	}
}

// TestBucketStore_RetryAfterUsesRequestedCount pins the §3.4 fix
// where the formula's numerator is the requested count, not 1. A
// SendMessageBatch of 10 against refillRate=1 with 0 tokens needs 10s
// to refill, not 1s — telling the client to wait 1s creates a busy-
// loop of premature retries that all reject again.
func TestBucketStore_RetryAfterUsesRequestedCount(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	for range 10 {
		store.charge(cfg, "orders", bucketActionSend, 1)
	}
	// Now batch of 10 against an empty bucket: needs 10s to refill.
	out := store.charge(cfg, "orders", bucketActionSend, 10)
	require.False(t, out.allowed)
	require.Equal(t, 10*time.Second, out.retryAfter)
}

// TestBucketStore_RetryAfterFloorWithSlowRefill pins the §3.4 rule
// for sub-1-RPS rates: SendRefillPerSecond=0.1 with 0 tokens needs
// 10s for the next single token, not 1s. This was the second of two
// Claude reviews caught on PR #664.
func TestBucketStore_RetryAfterFloorWithSlowRefill(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 0.1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	for range 10 {
		store.charge(cfg, "orders", bucketActionSend, 1)
	}
	out := store.charge(cfg, "orders", bucketActionSend, 1)
	require.False(t, out.allowed)
	require.Equal(t, 10*time.Second, out.retryAfter)
}

// TestBucketStore_ActionsHaveSeparateBuckets pins the (queue, action)
// granularity: a Send-bucket exhaustion does not leak into the Recv
// bucket's accounting and vice versa.
func TestBucketStore_ActionsHaveSeparateBuckets(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{
		SendCapacity: 10, SendRefillPerSecond: 1,
		RecvCapacity: 10, RecvRefillPerSecond: 1,
	}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	// Drain Send.
	for range 10 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	}
	require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	// Recv must still have full capacity.
	for range 10 {
		require.True(t, store.charge(cfg, "orders", bucketActionReceive, 1).allowed)
	}
}

// TestBucketStore_QueuesHaveSeparateBuckets pins per-queue isolation:
// a noisy queue does not consume another queue's budget.
func TestBucketStore_QueuesHaveSeparateBuckets(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	for range 10 {
		store.charge(cfg, "orders", bucketActionSend, 1)
	}
	require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	// Other queue, same cfg → fresh bucket.
	for range 10 {
		require.True(t, store.charge(cfg, "events", bucketActionSend, 1).allowed)
	}
}

// TestBucketStore_DefaultBucketCovers covers the §3.2 "Default*"
// fallback: a verb that doesn't match Send or Recv falls through to
// Default, allowing operators to set one cap that covers everything.
func TestBucketStore_DefaultBucketCovers(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{
		DefaultCapacity: 5, DefaultRefillPerSecond: 1,
	}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	for range 5 {
		require.True(t, store.charge(cfg, "orders", bucketActionAny, 1).allowed)
	}
	require.False(t, store.charge(cfg, "orders", bucketActionAny, 1).allowed)
	// And Send falls through to Default too when only Default is set.
	for range 5 {
		require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed,
			"Send falls through to Default which is empty")
	}
}

// TestBucketStore_ReconcilesBucketOnConfigChange pins the Codex P1
// fix on PR #679: a cached bucket whose capacity/refillRate no
// longer match the queue's current Throttle config gets rebuilt on
// the next charge() call. Without this, a node that loses leadership
// during a SetQueueAttributes commit and regains it later would keep
// enforcing the prior leader-term's limits — the SetQueueAttributes
// invalidation only runs on the leader that processed the commit,
// so a different leader's stale buckets survive.
func TestBucketStore_ReconcilesBucketOnConfigChange(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	cfgOld := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	// Drain the old bucket entirely.
	for range 10 {
		require.True(t, store.charge(cfgOld, "orders", bucketActionSend, 1).allowed)
	}
	require.False(t, store.charge(cfgOld, "orders", bucketActionSend, 1).allowed,
		"sanity: old config bucket exhausted")
	// Now charge with a NEW config — capacity 100, refill 50. The
	// bucket reconciliation must spot the cap/refill mismatch and
	// rebuild a fresh bucket at the new full capacity.
	cfgNew := &sqsQueueThrottle{SendCapacity: 100, SendRefillPerSecond: 50}
	for range 100 {
		require.True(t, store.charge(cfgNew, "orders", bucketActionSend, 1).allowed,
			"new config charge must succeed against a fresh bucket; stale-bucket bug would reject")
	}
	// 101st must reject under the new cap.
	require.False(t, store.charge(cfgNew, "orders", bucketActionSend, 1).allowed)
}

// TestBucketStore_ConcurrentReconciliationRespectsNewCapacity pins
// the CompareAndDelete fix on PR #679 round 4: two concurrent
// goroutines hitting a stale bucket must not race each other into
// double-replacing the map entry. Without CompareAndDelete the
// second goroutine's unconditional Delete would evict the first
// goroutine's fresh bucket, leaving the second's fresh bucket
// behind — but the first's bucket is already being charged, so
// total charges across the mismatch window can exceed the new
// capacity.
//
// Race the test by having N goroutines each invoke charge() with
// the new config (post-mismatch) on the same (queue, action). The
// first one through builds the fresh bucket; every later one must
// observe the same fresh bucket and share its capacity. After all
// goroutines finish, total successful charges must equal exactly
// the new capacity — anything more means a Delete-after-replace
// orphaned a fresh bucket.
func TestBucketStore_ConcurrentReconciliationRespectsNewCapacity(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)

	// Seed the store with a stale bucket from cfgOld.
	cfgOld := &sqsQueueThrottle{SendCapacity: 5, SendRefillPerSecond: 1}
	for range 5 {
		require.True(t, store.charge(cfgOld, "orders", bucketActionSend, 1).allowed)
	}

	// Now race many goroutines through the new config. Each charge
	// triggers reconciliation against cfgNew. The race window is
	// between Load detecting the stale bucket and CompareAndDelete +
	// LoadOrStore committing the replacement; without
	// CompareAndDelete, two racers can each Delete + LoadOrStore and
	// the loser's fresh bucket may end up orphaned while still being
	// charged through a leaked pointer.
	cfgNew := &sqsQueueThrottle{SendCapacity: 50, SendRefillPerSecond: 1}
	const goroutines = 200
	var (
		wg        sync.WaitGroup
		successes int64
		mu        sync.Mutex
	)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if store.charge(cfgNew, "orders", bucketActionSend, 1).allowed {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 50, successes,
		"exactly cfgNew.SendCapacity successes; a Delete-after-replace race would let some past the cap")
}

// TestBucketStore_SweepDoesNotEvictRecentlyUsedBucket pins the
// Codex P2 fix on PR #679 round 5: an earlier version of sweep
// computed idle, released bucket.mu, then unconditionally Deleted.
// A charge() running in that window could refill+take a token; the
// Delete then evicted the just-used bucket and the next request
// got a fresh full-capacity bucket — the just-taken token was
// effectively undone, allowing excess throughput.
//
// The test forces this exact race: pre-build a bucket with a stale
// lastRefill (well past cutoff), then in two paired goroutines
//
//	(A) advance the clock, run sweep
//	(B) run charge concurrently with sweep
//
// charge() acquires bucket.mu, refills, takes a token, releases.
// sweep() acquires bucket.mu after charge, observes the freshened
// lastRefill, and skips the delete. After both finish, the bucket
// must still be in the store with `tokens = capacity - 1`.
func TestBucketStore_SweepDoesNotEvictRecentlyUsedBucket(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	clk := now
	store := newBucketStore(func() time.Time { return clk }, time.Hour)
	cfg := &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}
	// Build the bucket via a single charge so it lands in the store.
	require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	// Backdate the bucket's lastRefill 2 hours so it's idle relative
	// to the 1h evict window.
	key := bucketKey{queue: "orders", action: bucketActionSend}
	v, ok := store.buckets.Load(key)
	require.True(t, ok)
	bucket, _ := v.(*tokenBucket)
	bucket.mu.Lock()
	bucket.lastRefill = now.Add(-2 * time.Hour)
	bucket.mu.Unlock()
	// Race sweep against charge. The "advanced clock" makes lastRefill
	// older than cutoff so sweep would delete unconditionally; the
	// concurrent charge must update lastRefill before sweep observes
	// it.
	clk = now.Add(2 * time.Hour)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		store.sweep()
	}()
	go func() {
		defer wg.Done()
		// Hammer the same bucket so at least one charge interleaves
		// with sweep's bucket.mu acquisition.
		for range 50 {
			store.charge(cfg, "orders", bucketActionSend, 1)
		}
	}()
	wg.Wait()
	// The bucket must still be in the store: at least one of the 50
	// charges updated lastRefill within the sweep window, so the
	// re-check under bucket.mu sees the freshened timestamp and
	// CompareAndDelete is skipped. If the old code (compute-then-
	// delete-without-recheck) regresses, the bucket may be evicted
	// and the assertion fails.
	_, stillThere := store.buckets.Load(key)
	require.True(t, stillThere,
		"sweep must not evict a bucket that was used during the sweep window")
}

// TestComputeRetryAfter_CapsAtMaximum pins the Gemini medium fix on
// PR #679: a tiny refillRate (e.g. 1e-9) plus a large requested
// count would otherwise compute a multi-day Retry-After and
// time.Duration arithmetic could overflow. Capped at
// throttleRetryAfterCap so the client always sees a sane value.
func TestComputeRetryAfter_CapsAtMaximum(t *testing.T) {
	t.Parallel()
	got := computeRetryAfter(1, 0, 1e-9)
	require.Equal(t, throttleRetryAfterCap, got,
		"computeRetryAfter must cap at throttleRetryAfterCap regardless of input")
}

// TestThrottleAttributesPresent covers the request-gate helper used
// by setQueueAttributes to skip cache invalidation on unrelated
// updates (Codex P1 on PR #679).
func TestThrottleAttributesPresent(t *testing.T) {
	t.Parallel()
	require.False(t, throttleAttributesPresent(map[string]string{}))
	require.False(t, throttleAttributesPresent(map[string]string{"VisibilityTimeout": "30"}))
	require.True(t, throttleAttributesPresent(map[string]string{"ThrottleSendCapacity": "10"}))
	require.True(t, throttleAttributesPresent(map[string]string{"ThrottleRecvRefillPerSecond": "5"}))
	require.True(t, throttleAttributesPresent(map[string]string{"ThrottleDefaultCapacity": "5"}))
}

// TestBucketStore_InvalidateQueueDropsAllActions pins the §3.1 cache
// invalidation contract for SetQueueAttributes / DeleteQueue: every
// bucket belonging to the queue is dropped, even ones not currently
// being charged. A future verb that grows a new bucket can't sneak
// past invalidation by being wired into one site only.
func TestBucketStore_InvalidateQueueDropsAllActions(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{
		SendCapacity: 10, SendRefillPerSecond: 1,
		RecvCapacity: 10, RecvRefillPerSecond: 1,
	}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	// Drain both buckets.
	for range 10 {
		store.charge(cfg, "orders", bucketActionSend, 1)
		store.charge(cfg, "orders", bucketActionReceive, 1)
	}
	require.False(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
	require.False(t, store.charge(cfg, "orders", bucketActionReceive, 1).allowed)
	// Invalidate.
	store.invalidateQueue("orders")
	// Both buckets must now be at full capacity again.
	for range 10 {
		require.True(t, store.charge(cfg, "orders", bucketActionSend, 1).allowed)
		require.True(t, store.charge(cfg, "orders", bucketActionReceive, 1).allowed)
	}
}

// TestBucketStore_ConcurrentChargesPreserveCount pins the concurrency
// contract under -race: 100 goroutines race for tokens against a
// capacity-50 bucket. Exactly 50 must succeed; the other 50 must be
// rejected. Anything else (101 successes, partial-credit consumption
// during reject) means the per-bucket mutex is broken.
func TestBucketStore_ConcurrentChargesPreserveCount(t *testing.T) {
	t.Parallel()
	cfg := &sqsQueueThrottle{SendCapacity: 50, SendRefillPerSecond: 1}
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	store := newBucketStore(func() time.Time { return now }, time.Hour)
	var (
		wg        sync.WaitGroup
		successes int64
		mu        sync.Mutex
	)
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if store.charge(cfg, "orders", bucketActionSend, 1).allowed {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 50, successes,
		"exactly capacity successes; broken mutex would let some race past or double-charge")
}

// --- Validator tests ---

// TestValidateThrottleConfig_NilOrEmpty is the no-op: a meta with no
// Throttle, or with the zero-valued struct, validates clean and gets
// canonicalised so downstream code only has to handle the nil case.
func TestValidateThrottleConfig_NilOrEmpty(t *testing.T) {
	t.Parallel()
	m := &sqsQueueMeta{}
	require.NoError(t, validateThrottleConfig(m))
	require.Nil(t, m.Throttle)
	m.Throttle = &sqsQueueThrottle{}
	require.NoError(t, validateThrottleConfig(m))
	require.Nil(t, m.Throttle, "all-zero post-validate must canonicalise to nil")
}

// TestValidateThrottleConfig_BothZeroOrBothPositive pins the §3.2
// pair-wise rule: an action's capacity and refill must agree on
// whether the action is enabled.
func TestValidateThrottleConfig_BothZeroOrBothPositive(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		cfg     sqsQueueThrottle
		wantErr bool
	}{
		{"send capacity without refill", sqsQueueThrottle{SendCapacity: 10}, true},
		{"send refill without capacity", sqsQueueThrottle{SendRefillPerSecond: 1}, true},
		{"recv capacity without refill", sqsQueueThrottle{RecvCapacity: 10}, true},
		{"recv refill without capacity", sqsQueueThrottle{RecvRefillPerSecond: 1}, true},
		{"both positive ok", sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.cfg
			err := validateThrottleConfig(&sqsQueueMeta{Throttle: &cfg})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateThrottleConfig_CapacityGEMaxBatchCharge pins the §3.2
// floor for batch-covered actions: SendMessageBatch and
// DeleteMessageBatch each charge up to 10 tokens, so a capacity below
// 10 makes every full batch permanently unserviceable.
func TestValidateThrottleConfig_CapacityGEMaxBatchCharge(t *testing.T) {
	t.Parallel()
	err := validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{SendCapacity: 5, SendRefillPerSecond: 1},
	})
	require.Error(t, err)
	err = validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{RecvCapacity: 9, RecvRefillPerSecond: 1},
	})
	require.Error(t, err)
	err = validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 1},
	})
	require.NoError(t, err)
}

// TestValidateThrottleConfig_DefaultBucketBatchFloor pins the
// Codex P1 fix on PR #679 round 5: Default* gets the same batch-
// capacity ≥ 10 floor as Send/Recv because resolveActionConfig
// falls Send/Recv traffic through to Default when the dedicated
// pair is unset. Without the floor a Default-only config of
// {capacity=5, refill=1} would accept SendMessageBatch entries=10
// requests at the validator and reject them forever at the bucket.
func TestValidateThrottleConfig_DefaultBucketBatchFloor(t *testing.T) {
	t.Parallel()
	// Capacity 1 (below the batch floor) must reject.
	err := validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{DefaultCapacity: 1, DefaultRefillPerSecond: 1},
	})
	require.Error(t, err)
	// Capacity below batch floor at 5 must also reject.
	err = validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{DefaultCapacity: 5, DefaultRefillPerSecond: 1},
	})
	require.Error(t, err)
	// Capacity exactly at the batch floor is accepted.
	err = validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{DefaultCapacity: 10, DefaultRefillPerSecond: 1},
	})
	require.NoError(t, err)
}

// TestValidateThrottleConfig_CapacityGERefill pins the §3.2 burst
// rule: capacity below refill makes the bucket unable to accumulate
// any burst headroom — the capacity floor is the refill rate.
func TestValidateThrottleConfig_CapacityGERefill(t *testing.T) {
	t.Parallel()
	err := validateThrottleConfig(&sqsQueueMeta{
		Throttle: &sqsQueueThrottle{SendCapacity: 10, SendRefillPerSecond: 50},
	})
	require.Error(t, err)
}

// TestParseThrottleFloat_RejectsBadInputs covers the per-field
// parser: NaN, infinity, negative values, malformed strings, and the
// hard ceiling all reject with InvalidAttributeValue.
func TestParseThrottleFloat_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	bad := []string{
		"",
		"not a number",
		"NaN",
		"Inf",
		"-1",
		"-0.5",
		"1e100",     // > hard ceiling
		"100000.01", // > hard ceiling by epsilon
	}
	for _, in := range bad {
		t.Run(in, func(t *testing.T) {
			t.Parallel()
			_, err := parseThrottleFloat(in)
			require.Error(t, err, "input %q must be rejected", in)
		})
	}
	// Boundary: hard ceiling exactly is accepted.
	v, err := parseThrottleFloat("100000")
	require.NoError(t, err)
	require.Equal(t, 100000.0, v)
}

// TestComputeRetryAfter_FloorsAtOneSecond pins the §3.4 minimum-1
// floor: HTTP/1.1 §10.2.3 specifies integer-second granularity, so
// even a sub-second wait is rounded up to 1.
func TestComputeRetryAfter_FloorsAtOneSecond(t *testing.T) {
	t.Parallel()
	// needed=0.5, refill=10 → ceil(0.05) = 1
	require.Equal(t, time.Second, computeRetryAfter(1, 0.5, 10))
	// needed=1, refill=100 → ceil(0.01) = 1
	require.Equal(t, time.Second, computeRetryAfter(1, 0, 100))
}
