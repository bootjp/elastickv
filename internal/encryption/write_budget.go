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

	// writeBudgetPercentBase is the denominator writeBudgetRefusePercent
	// is expressed against.
	writeBudgetPercentBase = 100
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

	// WriteBudgetExhausted — at or past the ceiling itself.
	//
	// Because Record reserves a slot with CAS, the counter stops at
	// the refusal threshold, so a ceiling above that threshold is
	// never reached through Record. This verdict is the fail-closed
	// guard for the cases that can still land on it: a degenerate
	// ceiling whose threshold equals it, and any future path that
	// raises the counter without going through admission control (a
	// count restored from disk, say). It must never silently become
	// Allow.
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
		ceiling:   ceiling,
		threshold: refusalThreshold(ceiling),
		counters:  make(map[uint32]*atomic.Uint64),
	}
}

// refusalThreshold returns floor(ceiling * writeBudgetRefusePercent /
// writeBudgetPercentBase) without overflowing and without discarding
// the remainder.
//
// Dividing first and multiplying after loses up to 99 ceiling-units of
// precision, which is invisible at the 2^32 default but severe for a
// smaller configured ceiling: a ceiling of 199 would refuse at 90
// (about 45%), and anything under 100 would refuse at 0 -- i.e. refuse
// the very first write and wedge the DEK. Splitting the quotient and
// the remainder keeps the exact floor: with ceiling = 100q + r, the
// result is 90q + floor(9r/10), and r < 100 bounds the remainder term
// at 8910 so neither term can overflow.
func refusalThreshold(ceiling uint64) uint64 {
	exact := ceiling/writeBudgetPercentBase*writeBudgetRefusePercent +
		ceiling%writeBudgetPercentBase*writeBudgetRefusePercent/writeBudgetPercentBase
	if exact == 0 {
		// A ceiling below 2 rounds the 90% point down to zero. Refusing
		// every write is worse than refusing slightly late: it would
		// wedge the DEK and loop on rotation proposals that can never
		// make progress. Allow exactly one write instead.
		return 1
	}
	return exact
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
	// The slot is RESERVED with CAS rather than incremented after the
	// fact. Load-then-Add lets every writer that read a count below the
	// threshold increment it, so writers that are then refused still
	// consume budget: with a ceiling of 100 and the count at 89,
	// eleven concurrent calls leave the counter at 100 having permitted
	// one write, and the next call reports Exhausted for a DEK that
	// issued 90 writes. CAS makes the decision and the increment one
	// step, so the counter only ever records writes that were allowed.
	for {
		used := counter.Load()
		if used >= b.ceiling {
			return WriteBudgetExhausted
		}
		if used >= b.threshold {
			return WriteBudgetRotate
		}
		if counter.CompareAndSwap(used, used+1) {
			return WriteBudgetAllow
		}
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
