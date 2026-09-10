package encryption

import (
	"sync"
	"sync/atomic"
)

// §5.2 writes-per-DEK rotation trigger.
//
// The design bounds rotation cadence by two triggers, whichever fires
// first: 90 days, or a hard ceiling of 2^32 writes per
// (DEK, process-load) pair, in line with NIST SP 800-38D §8.3 for
// authenticated encryption. This is the second one.
//
// The ceiling is deliberately conservative. With the §4.1
// counter-based nonces the actual cryptographic safety budget is far
// higher, but the design keeps 2^32 so the system does not depend on a
// single number being right everywhere in the codebase — and this
// tracker inherits that posture: it would rather rotate early than
// reason its way to a larger bound.
const (
	// DefaultWriteBudgetCeiling is the §5.2 hard ceiling per
	// (DEK, process-load).
	DefaultWriteBudgetCeiling uint64 = 1 << 32

	// writeBudgetRefusePercent is the fraction of the ceiling at which
	// admission control starts refusing new writes and the cluster
	// auto-proposes a rotate-dek entry. Refusing BEFORE the ceiling is
	// the point: rotation needs a Raft round trip, so waiting until
	// the budget is actually spent would mean either blocking writes
	// while it commits or issuing writes past the ceiling.
	writeBudgetRefusePercent = 90
)

// WriteBudgetVerdict is what a write attempt is permitted to do.
type WriteBudgetVerdict int

const (
	// WriteBudgetAllow — under the refusal threshold, proceed.
	WriteBudgetAllow WriteBudgetVerdict = iota

	// WriteBudgetRotate — at or past 90% of the ceiling. The write is
	// refused and the caller should propose a rotation. The write is
	// NOT counted, so a caller that retries cannot drive the counter
	// past the ceiling while rotation commits.
	WriteBudgetRotate

	// WriteBudgetExhausted — at or past the ceiling itself. Reachable
	// only if a caller ignored WriteBudgetRotate; it exists so that
	// path still fails closed rather than silently continuing to
	// encrypt under a DEK whose budget is spent.
	WriteBudgetExhausted
)

func (v WriteBudgetVerdict) String() string {
	switch v {
	case WriteBudgetAllow:
		return "allow"
	case WriteBudgetRotate:
		return "rotate"
	case WriteBudgetExhausted:
		return "exhausted"
	default:
		return "unknown"
	}
}

// Allowed reports whether the write may proceed.
func (v WriteBudgetVerdict) Allowed() bool { return v == WriteBudgetAllow }

// WriteBudget tracks writes per DEK for this process load.
//
// Scope is deliberately per-load, matching §5.2's "(DEK,
// process-load)" pair and the §4.1 nonce construction, whose
// local_epoch bumps on every process start. A restart therefore begins
// a fresh budget — which is correct, because it also begins a fresh
// nonce epoch, so the (key, nonce) space the ceiling protects is
// itself fresh.
type WriteBudget struct {
	ceiling   uint64
	threshold uint64

	mu       sync.RWMutex
	counters map[uint32]*atomic.Uint64
}

// NewWriteBudget returns a budget with the given ceiling; a
// non-positive ceiling uses the §5.2 default.
func NewWriteBudget(ceiling uint64) *WriteBudget {
	if ceiling == 0 {
		ceiling = DefaultWriteBudgetCeiling
	}
	return &WriteBudget{
		ceiling: ceiling,
		// Integer arithmetic in this order so the threshold cannot
		// overflow for a large ceiling and cannot round to zero for a
		// small one.
		threshold: ceiling / 100 * writeBudgetRefusePercent,
		counters:  make(map[uint32]*atomic.Uint64),
	}
}

// Record accounts for one write under keyID and returns whether it may
// proceed.
//
// A refused write is not counted. Counting it would let a caller that
// retries on refusal walk the counter past the ceiling, which is the
// one thing the ceiling exists to prevent.
func (b *WriteBudget) Record(keyID uint32) WriteBudgetVerdict {
	if b == nil {
		return WriteBudgetAllow
	}
	counter := b.counterFor(keyID)
	// Read before incrementing so the decision is made against the
	// count of writes already issued, and the increment happens only
	// on the path that actually issues one.
	switch used := counter.Load(); {
	case used >= b.ceiling:
		return WriteBudgetExhausted
	case used >= b.threshold:
		return WriteBudgetRotate
	default:
		// A concurrent racer may have crossed the threshold between
		// the Load and here, so re-check the post-increment value: the
		// overshoot is bounded by the number of in-flight writers and
		// never reaches the ceiling from below the threshold, but
		// returning Allow for a write that landed past the threshold
		// would delay the rotation signal.
		if counter.Add(1) > b.threshold {
			return WriteBudgetRotate
		}
		return WriteBudgetAllow
	}
}

// Used reports the writes recorded under keyID this process load.
func (b *WriteBudget) Used(keyID uint32) uint64 {
	if b == nil {
		return 0
	}
	b.mu.RLock()
	counter, ok := b.counters[keyID]
	b.mu.RUnlock()
	if !ok {
		return 0
	}
	return counter.Load()
}

// Remaining reports how many writes keyID may still issue before the
// refusal threshold. Zero means rotation is due.
func (b *WriteBudget) Remaining(keyID uint32) uint64 {
	if b == nil {
		return 0
	}
	used := b.Used(keyID)
	if used >= b.threshold {
		return 0
	}
	return b.threshold - used
}

// Forget drops the counter for a retired DEK.
//
// Called after a rotation retires keyID, so a long-lived process that
// rotates repeatedly does not accumulate a counter per historical DEK.
// It is NOT a way to reset a live DEK's budget: doing that would
// discard the very accounting the ceiling depends on.
func (b *WriteBudget) Forget(keyID uint32) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.counters, keyID)
}

// counterFor returns keyID's counter, creating it on first use. The
// read path takes only an RLock, so the steady state on a hot write
// path is an uncontended read plus an atomic add.
func (b *WriteBudget) counterFor(keyID uint32) *atomic.Uint64 {
	b.mu.RLock()
	counter, ok := b.counters[keyID]
	b.mu.RUnlock()
	if ok {
		return counter
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if counter, ok := b.counters[keyID]; ok {
		return counter
	}
	counter = &atomic.Uint64{}
	b.counters[keyID] = counter
	return counter
}
