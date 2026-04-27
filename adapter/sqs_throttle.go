package adapter

import (
	"context"
	"math"
	"net/http"
	"sync"
	"time"
)

// Per-queue throttling — token-bucket store that hangs off *SQSServer.
// See docs/design/2026_04_26_proposed_sqs_per_queue_throttling.md for
// the full design and rollout context. This file implements §3.1 (bucket
// store + token bucket), §3.3 (charging model), §3.4 (Throttling
// envelope helpers) and the cache-invalidation primitives §3.1 calls
// out for SetQueueAttributes / DeleteQueue.

// Canonical bucket-action vocabulary. The JSON field-name prefixes
// (Send*, Recv*, Default*) are the operator-facing contract; these
// values are what the in-memory map is keyed on. The mapping is fixed
// per the design's §3.2 "Config-field → bucket-action mapping" table.
const (
	bucketActionSend    = "Send"
	bucketActionReceive = "Receive"
	bucketActionAny     = "*"
)

// throttleAllActions is the canonical list used by every cache
// invalidation site. Defined once here so a future verb that grows a
// new bucket cannot land in production with one site forgetting to
// invalidate it.
var throttleAllActions = []string{bucketActionSend, bucketActionReceive, bucketActionAny}

// throttleAttributeNames is the wire-side set of Throttle*
// attributes a SetQueueAttributes request can carry. Used by the
// invalidation gate in setQueueAttributes so an unrelated update
// (e.g. VisibilityTimeout only) does not pay the cache invalidation
// cost or, worse, give the caller a way to silently reset bucket
// state via a no-op SetQueueAttributes (Codex P1 on PR #679).
var throttleAttributeNames = []string{
	"ThrottleSendCapacity",
	"ThrottleSendRefillPerSecond",
	"ThrottleRecvCapacity",
	"ThrottleRecvRefillPerSecond",
	"ThrottleDefaultCapacity",
	"ThrottleDefaultRefillPerSecond",
}

// throttleAttributesPresent reports whether attrs carries any
// Throttle* key. Cheap O(6) check; the throttleAttributeNames slice
// is the source of truth so a future Throttle* attribute name added
// in one place automatically participates in the gate.
func throttleAttributesPresent(attrs map[string]string) bool {
	for _, k := range throttleAttributeNames {
		if _, ok := attrs[k]; ok {
			return true
		}
	}
	return false
}

// throttleHardCeilingPerSecond bounds any user-supplied capacity or
// refill rate. A typo like SendCapacity=1e9 silently meaning "no limit"
// is more dangerous than an explicit InvalidAttributeValue (Codex P1 on
// PR #664: a wide-open queue masks itself as "throttled").
const throttleHardCeilingPerSecond = 100_000.0

// throttleMinBatchCapacity is the smallest acceptable per-action
// capacity once the action covers a batch verb. SendMessageBatch and
// DeleteMessageBatch each charge up to 10 tokens (AWS caps batch size
// at 10), so a SendCapacity below 10 makes every full batch
// permanently unserviceable.
const throttleMinBatchCapacity = float64(sqsBatchMaxEntries)

// throttleIdleEvictAfter is the idle window after which a quiet bucket
// is dropped from the in-memory store. A background goroutine
// (runSweepLoop) fires the eviction sweep on each
// throttleEvictSweepEvery tick; the hot path never calls sweep().
// A queue that resumes activity rebuilds its bucket from the meta
// record at full capacity, matching the failover semantics
// documented in §3.1.
const throttleIdleEvictAfter = time.Hour

// throttleEvictSweepEvery is the interval at which runSweepLoop fires
// the idle-evict sweep in its background goroutine. The hot-path
// charge() never calls into the sweep so a many-queue cluster pays
// the O(N) cost only on the goroutine's tick, never on a request.
const throttleEvictSweepEvery = time.Minute

// bucketKey is the in-memory map key.
type bucketKey struct {
	queue  string
	action string
}

// tokenBucket is one bucket's mutable state. mu is per-bucket so
// concurrent traffic on different queues never serialises on the same
// lock; refill + take + release of a single bucket is the only
// critical section. Never held across the bucketStore's sync.Map.
type tokenBucket struct {
	mu         sync.Mutex
	capacity   float64
	refillRate float64
	tokens     float64
	lastRefill time.Time
}

// bucketStore holds every active bucket for an SQS server process.
// sync.Map matches the read-mostly access pattern: lookups are nearly
// always Load hits; LoadOrStore pays the write cost only on first use.
//
// The idle-evict sweep runs from runSweepLoop on a background ticker
// — there is no hot-path serialisation primitive because the only
// caller of sweep() is the sole goroutine the ticker drives.
type bucketStore struct {
	buckets      sync.Map // map[bucketKey]*tokenBucket
	clock        func() time.Time
	evictedAfter time.Duration
	sweepEvery   time.Duration
}

// newBucketStore constructs a store whose clock + idle-evict window
// can be overridden for tests. The sweep cadence is fixed at
// throttleEvictSweepEvery; tests that want a different cadence have
// no use case yet (the sweep itself is a low-cost no-op when the
// store is small). Production calls newBucketStoreDefault.
func newBucketStore(clock func() time.Time, evictedAfter time.Duration) *bucketStore {
	if clock == nil {
		clock = time.Now
	}
	return &bucketStore{
		clock:        clock,
		evictedAfter: evictedAfter,
		sweepEvery:   throttleEvictSweepEvery,
	}
}

// newBucketStoreDefault uses the production constants. Kept as a
// separate constructor so test wiring stays explicit about the
// time-window overrides.
func newBucketStoreDefault() *bucketStore {
	return newBucketStore(time.Now, throttleIdleEvictAfter)
}

// chargeOutcome is returned from charge so the caller can build the
// Throttling envelope (Retry-After computed from refillRate +
// requestedCount, see §3.4) without re-loading the bucket.
type chargeOutcome struct {
	allowed       bool
	retryAfter    time.Duration
	tokensAfter   float64
	bucketPresent bool
}

// charge takes count tokens from the bucket identified by (queue,
// action) using cfg as the source-of-truth for capacity / refillRate.
// cfg may be nil — in which case throttling is disabled for the queue
// and charge returns allowed=true without touching the map.
//
// count must be ≥ 1; the caller has already validated batch size at
// the request layer (sqs_messages_batch.go bounds it to
// sqsBatchMaxEntries).
func (b *bucketStore) charge(cfg *sqsQueueThrottle, queue, action string, count int) chargeOutcome {
	if b == nil || cfg == nil || cfg.IsEmpty() {
		// Throttling disabled (default): every request allowed, no
		// bucket allocated. The hot path stays a single nil-check.
		return chargeOutcome{allowed: true, bucketPresent: false}
	}
	resolvedAction, capacity, refill := resolveActionConfig(cfg, action)
	if capacity == 0 || refill == 0 {
		// This action has no throttle configured (e.g. only Send is
		// configured and the request is a Recv). Default* covers any
		// remaining unconfigured action; if Default* is also zero the
		// request is unthrottled.
		return chargeOutcome{allowed: true, bucketPresent: false}
	}
	if count < 1 {
		count = 1
	}
	// Bucket key uses the *resolved* action so Send-falls-through-to-
	// Default and Recv-falls-through-to-Default share the same Default
	// bucket. Without the resolution, an operator who configures only
	// Default would still get one bucket per requesting action — three
	// independent quotas instead of one shared cap.
	bucket := b.loadOrInit(queue, resolvedAction, capacity, refill)

	now := b.clock()
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	// Refill before reading: tokens accrue at refillRate * elapsed,
	// capped at the configured capacity. This is the single place that
	// advances tokens forward in time so the "fresh bucket on failover"
	// guarantee from §3.1 holds: a new leader's bucket starts at full
	// capacity and refills only based on elapsed time on this process.
	if elapsed := now.Sub(bucket.lastRefill).Seconds(); elapsed > 0 {
		bucket.tokens += elapsed * bucket.refillRate
		if bucket.tokens > bucket.capacity {
			bucket.tokens = bucket.capacity
		}
		bucket.lastRefill = now
	}
	requested := float64(count)
	if bucket.tokens >= requested {
		bucket.tokens -= requested
		return chargeOutcome{allowed: true, tokensAfter: bucket.tokens, bucketPresent: true}
	}
	// Reject the whole batch — partial throttling within a batch is
	// hard to reason about and AWS rejects the whole call.
	return chargeOutcome{
		allowed:       false,
		retryAfter:    computeRetryAfter(requested, bucket.tokens, bucket.refillRate),
		tokensAfter:   bucket.tokens,
		bucketPresent: true,
	}
}

// loadOrInit handles the first-use insert race. Two concurrent first
// requests for the same (queue, action) both arrive at LoadOrStore;
// one wins and the loser's freshly-built bucket is discarded. This is
// safe because both racers compute identical (capacity, refillRate)
// from the same meta snapshot — the bucket they would build is
// behaviourally interchangeable.
//
// Reconciliation against stale config (Codex P1 on PR #679): if a
// cached bucket's capacity/refillRate differ from the cfg's current
// values, the bucket is replaced with a fresh one built from the
// current config. Without this check, a node that lost leadership
// during a SetQueueAttributes commit and then regained leadership
// later would keep enforcing the prior leader-term's limits — the
// SetQueueAttributes invalidation only runs on the leader that
// processed the commit, so a different leader's stale buckets
// survive. The reconciliation also covers the case where the
// invalidation gate in setQueueAttributes is bypassed (e.g. by a
// future admin path that mutates throttle config without touching
// SetQueueAttributes).
func (b *bucketStore) loadOrInit(queue, action string, capacity, refill float64) *tokenBucket {
	key := bucketKey{queue: queue, action: action}
	if v, ok := b.buckets.Load(key); ok {
		// type assertion is sound: only tokenBucket pointers are stored.
		bucket, _ := v.(*tokenBucket)
		// Cheap field comparison under the bucket's own lock — if the
		// cached bucket matches the current config we return it
		// directly. A mismatch means the on-disk meta moved while
		// this node held a stale bucket; rebuild from the current
		// config (full capacity, matching the failover semantics).
		bucket.mu.Lock()
		matches := bucket.capacity == capacity && bucket.refillRate == refill
		bucket.mu.Unlock()
		if matches {
			return bucket
		}
		// CompareAndDelete is mandatory here: an unconditional Delete
		// races against a concurrent goroutine that already detected
		// the same mismatch and replaced the entry with its own fresh
		// bucket — our Delete would evict its fresh entry, then our
		// LoadOrStore would put another fresh bucket. The map ends up
		// holding our bucket, but the racer's bucket might have
		// already been handed out via LoadOrStore to a third
		// goroutine that is now charging a bucket no longer in the
		// map, while later requests get a different fresh bucket at
		// full capacity. CompareAndDelete makes our Delete a no-op
		// when the map already holds someone else's fresh bucket.
		// (Claude P1 on PR #679 round 4 caught this.)
		b.buckets.CompareAndDelete(key, v)
		// fall through to LoadOrStore — a concurrent racer might
		// have already inserted a fresh bucket with the current
		// config, in which case LoadOrStore picks it up and the new
		// bucket below is discarded.
	}
	now := b.clock()
	fresh := &tokenBucket{
		capacity:   capacity,
		refillRate: refill,
		tokens:     capacity, // start at full capacity, matches failover semantics.
		lastRefill: now,
	}
	actual, _ := b.buckets.LoadOrStore(key, fresh)
	bucket, _ := actual.(*tokenBucket)
	return bucket
}

// invalidateQueue drops every bucket belonging to the named queue.
// Called *after* the Raft commit on SetQueueAttributes / DeleteQueue
// so the next request rebuilds from the freshly committed meta. The
// LoadOrStore race a concurrent in-flight request might run with the
// old bucket is benign: the rebuilt bucket starts at full capacity
// (same as failover), the old request's outcome is unaffected.
func (b *bucketStore) invalidateQueue(queue string) {
	if b == nil {
		return
	}
	for _, action := range throttleAllActions {
		b.buckets.Delete(bucketKey{queue: queue, action: action})
	}
}

// runSweepLoop runs the idle-evict sweep on a background ticker so
// the request hot path never pays the O(N) sync.Map.Range cost
// (Gemini high on PR #679: a many-queue cluster would see latency
// spikes on whichever request was unlucky enough to trigger the
// per-minute on-hot-path sweep). Returns when ctx is done — the
// SQSServer wires this to s.reaperCtx so a Stop() call cleans the
// goroutine up alongside the existing reaper.
func (b *bucketStore) runSweepLoop(ctx context.Context) {
	if b == nil || b.evictedAfter <= 0 || b.sweepEvery <= 0 {
		return
	}
	t := time.NewTicker(b.sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.sweep()
		}
	}
}

// sweep walks the bucket store dropping any bucket idle longer than
// evictedAfter. Called from runSweepLoop on a background ticker —
// the ticker is the only caller, so sweep() does not need its own
// serialisation. Bucket lookups stay O(1) on the hot path; sweep
// iterates every entry under the per-bucket lock so it can re-check
// idle and the map entry atomically.
//
// Eviction race (Codex P2 on PR #679 round 5): an earlier version
// computed idle, released the lock, then unconditionally Deleted —
// a concurrent charge() running in that window could refill +
// take a token, the subsequent Delete would evict the just-used
// bucket, and the next request would get a fresh full-capacity
// bucket so the deduction was effectively undone. The fix has two
// parts:
//  1. Hold bucket.mu across the Delete so the idle observation
//     cannot be invalidated between check and delete. A concurrent
//     charge() that wants the bucket either runs to completion
//     before sweep acquires mu (sweep then sees the updated
//     lastRefill and skips delete), or waits for sweep to release
//     mu (charge then takes the bucket — but sweep has already
//     removed it from the map, so the charge succeeds against an
//     orphan bucket and only the next-after-charge request gets
//     the full-cap rebuild — bounded one-token leak).
//  2. CompareAndDelete with v ensures sweep does not evict a
//     replacement bucket inserted by invalidateQueue or a future
//     reconciliation path.
//
// Holding bucket.mu across sync.Map.Delete is safe — sync.Map.Load
// is wait-free on the read-only path and never blocks waiting for
// bucket.mu, so there is no AB-BA cycle with charge().
func (b *bucketStore) sweep() {
	cutoff := b.clock().Add(-b.evictedAfter)
	b.buckets.Range(func(k, v any) bool {
		bucket, _ := v.(*tokenBucket)
		bucket.mu.Lock()
		if bucket.lastRefill.Before(cutoff) {
			b.buckets.CompareAndDelete(k, v)
		}
		bucket.mu.Unlock()
		return true
	})
}

// resolveActionConfig maps a charge() action to (effective bucket
// action, capacity, refillRate) from cfg. Send* and Recv* keep their
// own buckets when configured; otherwise the action falls through to
// the Default bucket and gets the canonical "*" key so all
// fall-through actions share one bucket. Returning (_, 0, 0) means
// "no throttle for this action" and the caller short-circuits.
func resolveActionConfig(cfg *sqsQueueThrottle, action string) (string, float64, float64) {
	switch action {
	case bucketActionSend:
		if cfg.SendCapacity > 0 {
			return bucketActionSend, cfg.SendCapacity, cfg.SendRefillPerSecond
		}
	case bucketActionReceive:
		if cfg.RecvCapacity > 0 {
			return bucketActionReceive, cfg.RecvCapacity, cfg.RecvRefillPerSecond
		}
	}
	if cfg.DefaultCapacity > 0 {
		return bucketActionAny, cfg.DefaultCapacity, cfg.DefaultRefillPerSecond
	}
	return action, 0, 0
}

// throttleRetryAfterCap bounds the Retry-After value the client sees
// (Gemini medium on PR #679). Without a cap, a tiny refillRate plus
// a large requested count would compute a multi-day wait — and
// time.Duration arithmetic can overflow at the upper end. One hour
// matches the bucket store's idle-evict window: by the time the
// suggested retry would otherwise expire, the bucket would have
// been evicted and rebuilt at full capacity anyway, so a longer
// suggestion is meaningless. Producers that hit the cap are also
// strongly mis-configured; capping is a guard rail, not a feature.
const throttleRetryAfterCap = time.Hour

// computeRetryAfter implements the §3.4 formula:
//
//	needed              := requested - currentTokens
//	secondsToNextRefill := ceil(needed / refillRate)
//	retryAfter          := max(1, int(secondsToNextRefill))
//
// requested is the same count the charge step uses (1 for single-message
// verbs, len(Entries) for batch verbs). The min-1 floor matches the
// HTTP/1.1 §10.2.3 integer-second granularity. The validator keeps
// refillRate > 0 so no divide-by-zero guard is needed.
//
// Capped at throttleRetryAfterCap to bound time.Duration arithmetic
// against pathologically small refillRate / large requested values.
func computeRetryAfter(requested, current, refillRate float64) time.Duration {
	needed := requested - current
	if needed <= 0 {
		// Pathological — caller invoked us with allowed=false but
		// tokens >= requested. Treat as "wait one tick" rather than
		// zero so the client backs off at least once.
		return time.Second
	}
	secs := math.Ceil(needed / refillRate)
	if secs < 1 {
		secs = 1
	}
	// Cap before multiplying to avoid time.Duration overflow on
	// pathological inputs (e.g. refillRate just above zero).
	const capSecs = float64(throttleRetryAfterCap / time.Second)
	if secs > capSecs {
		secs = capSecs
	}
	return time.Duration(secs) * time.Second
}

// throttleChargeCount maps a request to the token count the bucket
// should be charged for. Single-message verbs charge 1; batch verbs
// charge len(Entries). The bucket store itself takes count as a
// parameter so this helper can stay close to the wire-protocol layer
// in the request path.
func throttleChargeCount(entries int) int {
	if entries < 1 {
		return 1
	}
	return entries
}

// chargeQueue is the per-handler entry point used by handlers that
// do not already load the queue meta themselves (deleteMessage,
// changeMessageVisibility, and their batch siblings). It loads the
// meta at a fresh read timestamp (Pebble cache makes this cheap) and
// runs the bucket store's charge against the queue's Throttle config.
//
// Handlers that DO load the meta themselves (sendMessage,
// sendMessageBatch, receiveMessage) should use chargeQueueWithThrottle
// to avoid the redundant load (Gemini high on PR #679).
//
// chargeQueue intentionally swallows missing-queue errors: the caller
// is going to discover that the queue does not exist a few lines
// later and respond with QueueDoesNotExist. Letting the throttle
// check race the catalog read avoids two lookups in the fast path.
//
// Designed to sit OUTSIDE the OCC transaction (§4.2): a rejected
// request never reaches the coordinator. The retry loop in
// sendMessageWithRetry et al. would otherwise busy-loop on a
// permanent rate-limit reject, burning leader CPU.
func (s *SQSServer) chargeQueue(w http.ResponseWriter, r *http.Request, queueName, action string, count int) bool {
	if s.throttle == nil {
		return true
	}
	throttle := s.queueThrottleConfig(r, queueName)
	return s.chargeQueueWithThrottle(w, queueName, action, count, throttle)
}

// chargeQueueWithThrottle is the variant for handlers that already
// have the throttle config in hand from their own meta load. Drops
// the per-request meta load chargeQueue does, addressing the Gemini
// high finding on PR #679 about redundant storage reads on the hot
// path.
func (s *SQSServer) chargeQueueWithThrottle(w http.ResponseWriter, queueName, action string, count int, throttle *sqsQueueThrottle) bool {
	if s.throttle == nil {
		return true
	}
	outcome := s.throttle.charge(throttle, queueName, action, count)
	if outcome.allowed {
		return true
	}
	writeSQSThrottlingError(w, queueName, action, outcome.retryAfter)
	return false
}

// queueThrottleConfig loads just the Throttle config off a queue's
// meta record. Returns nil on any error or missing-queue — the
// surrounding handler is responsible for surfacing those, and a nil
// throttle config short-circuits the charge to "allowed".
//
// Held as a method on *SQSServer so a test can swap the meta loader
// via the existing nextTxnReadTS / loadQueueMetaAt seam.
func (s *SQSServer) queueThrottleConfig(r *http.Request, queueName string) *sqsQueueThrottle {
	if s.store == nil {
		return nil
	}
	readTS := s.nextTxnReadTS(r.Context())
	meta, exists, err := s.loadQueueMetaAt(r.Context(), queueName, readTS)
	if err != nil || !exists || meta == nil {
		return nil
	}
	return meta.Throttle
}
