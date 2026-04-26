package adapter

import "sync"

// streamWaiterRegistry tracks goroutines blocked inside an XREAD BLOCK loop
// so an XADD on the same node can wake them up immediately instead of
// letting the poll-fallback timer run. Lookup is keyed by the user stream
// key (the same byte slice the client passed to XADD/XREAD); waiters list
// every key the caller is interested in. A signal on any registered key
// wakes the waiter, and the caller is expected to re-check via xreadOnce
// after waking — the channel only carries "something might have changed".
//
// A buffered channel of size 1 is sufficient because the caller always
// re-checks state after waking; coalescing multiple in-flight signals into
// one wake-up is correct. Send is non-blocking (default branch on send) so
// XADD never stalls when a waiter is already pending.
//
// The mutex is held only briefly: collecting a snapshot of the waiter set
// in Signal lets the slow path (channel sends) run lock-free, and avoids
// holding the mutex across user code. Register's release fn is idempotent
// (sync.Once) so deferred releases survive panic-style early returns.
type streamWaiterRegistry struct {
	mu      sync.Mutex
	waiters map[string]map[*streamWaiter]struct{}
}

type streamWaiter struct {
	C    chan struct{}
	keys []string
}

func newStreamWaiterRegistry() *streamWaiterRegistry {
	return &streamWaiterRegistry{
		waiters: map[string]map[*streamWaiter]struct{}{},
	}
}

// Register subscribes a waiter to every key in keys. Duplicate keys are
// deduplicated so a multi-key XREAD where the client repeats the same
// stream does not signal the same waiter twice. Returns the waiter handle
// and a release fn the caller MUST defer; the release fn is safe to invoke
// multiple times. Nil-safe: returns a never-fires waiter and a no-op
// release if the registry is nil (the caller's select still gets a
// well-typed channel and the fallback timer drives the loop). The nil
// path uses a buffered-1 channel for consistency with the non-nil path
// so any code that reaches Signal on a nil-registry waiter (test stubs
// only, in practice) coalesces the same way.
func (reg *streamWaiterRegistry) Register(keys [][]byte) (*streamWaiter, func()) {
	if reg == nil {
		return &streamWaiter{C: make(chan struct{}, 1)}, func() {}
	}
	w := &streamWaiter{
		C:    make(chan struct{}, 1),
		keys: dedupStreamKeys(keys),
	}
	reg.mu.Lock()
	for _, s := range w.keys {
		set := reg.waiters[s]
		if set == nil {
			set = map[*streamWaiter]struct{}{}
			reg.waiters[s] = set
		}
		set[w] = struct{}{}
	}
	reg.mu.Unlock()
	var releaseOnce sync.Once
	return w, func() {
		releaseOnce.Do(func() { reg.unregister(w) })
	}
}

func (reg *streamWaiterRegistry) unregister(w *streamWaiter) {
	reg.mu.Lock()
	for _, s := range w.keys {
		set := reg.waiters[s]
		if set == nil {
			continue
		}
		delete(set, w)
		if len(set) == 0 {
			delete(reg.waiters, s)
		}
	}
	reg.mu.Unlock()
}

// Signal wakes every waiter registered for key. Caller is XADD on the
// local node after dispatchElems returns; by that point the FSM has
// applied the entry and a re-running xreadOnce on the leader is
// guaranteed to see it. Nil-safe: test stubs that construct a
// RedisServer literal directly may leave streamWaiters unset, in
// which case Signal is a no-op.
//
// Cost shape: O(N) work per Signal where N is the waiter count for
// the key — collects the waiter snapshot under the lock, then
// non-blocking-sends outside the lock. With R XADDs/sec on a stream
// shared by N BLOCK waiters, total wake-up cost is O(N*R)
// xreadOnce calls/sec across the leader. For the production
// operating point that motivated this change (small N — typically
// one consumer per stream — moderate R), this is strictly better
// than the old N*100 polls/sec. A pathological fan-out scenario
// (many BLOCK waiters watching one hot stream, or one waiter with
// an unsatisfiable far-future afterID being woken on every XADD)
// can exceed the old busy-poll cost. The 100 ms fallback timer
// bounds the worst-case CPU under such patterns. AfterID-aware
// filtering at Signal time is a follow-up — see
// docs/design/2026_04_26_proposed_fsm_apply_observer.md.
func (reg *streamWaiterRegistry) Signal(key []byte) {
	if reg == nil {
		return
	}
	reg.mu.Lock()
	// staticcheck SA6001: indexing the map directly with the byte
	// conversion lets the compiler skip the temporary string allocation
	// on the lookup-only path.
	set := reg.waiters[string(key)]
	if len(set) == 0 {
		reg.mu.Unlock()
		return
	}
	waiters := make([]*streamWaiter, 0, len(set))
	for w := range set {
		waiters = append(waiters, w)
	}
	reg.mu.Unlock()
	for _, w := range waiters {
		select {
		case w.C <- struct{}{}:
		default:
		}
	}
}

// dedupStreamKeys returns a new slice of stringified keys with duplicates
// removed, preserving first-seen order. Empty input returns an empty,
// non-nil slice so the caller can range over it without a nil-guard.
func dedupStreamKeys(keys [][]byte) []string {
	out := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		s := string(k)
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
