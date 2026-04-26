package adapter

import "sync"

// keyWaiterRegistry is a multi-key channel-based wakeup primitive for
// blocking Redis commands. It tracks goroutines blocked inside a wait
// loop (BZPOPMIN today; BLPOP / BRPOP / BLMOVE in follow-ups) so a
// later in-process write to one of the registered keys can wake them
// up immediately instead of letting the poll-fallback timer run.
//
// Lookup is keyed by the user key (the same byte slice the client
// passed to ZADD/BZPOPMIN). Waiters list every key the caller is
// interested in. A signal on any registered key wakes the waiter, and
// the caller is expected to re-check via the command's "try" helper
// after waking — the channel only carries "something might have
// changed".
//
// A buffered channel of size 1 is sufficient because the caller always
// re-checks state after waking; coalescing multiple in-flight signals
// into one wake-up is correct. Send is non-blocking (default branch
// on send) so ZADD never stalls when a waiter is already pending.
//
// The mutex is held only briefly: collecting a snapshot of the waiter
// set in Signal lets the slow path (channel sends) run lock-free, and
// avoids holding the mutex across user code. Register's release fn is
// idempotent (sync.Once) so deferred releases survive panic-style
// early returns.
//
// The registry is intentionally generic — the same type instance can
// be reused for any Redis blocking command. RedisServer holds one
// registry per domain (zsetWaiters today; streamWaiters when the
// independent perf/redis-event-driven-block PR consolidates) so a
// signal on a zset key does not iterate over stream waiters and vice
// versa.
type keyWaiterRegistry struct {
	mu      sync.Mutex
	waiters map[string]map[*keyWaiter]struct{}
}

type keyWaiter struct {
	C    chan struct{}
	keys []string
}

func newKeyWaiterRegistry() *keyWaiterRegistry {
	return &keyWaiterRegistry{
		waiters: map[string]map[*keyWaiter]struct{}{},
	}
}

// Register subscribes a waiter to every key in keys. Duplicate keys
// are deduplicated so a multi-key BZPOPMIN where the client repeats
// the same key does not signal the same waiter twice. Returns the
// waiter handle and a release fn the caller MUST defer; the release
// fn is safe to invoke multiple times. Nil-safe: returns a never-
// fires waiter and a no-op release if the registry is nil (the
// caller's select still gets a well-typed channel and the fallback
// timer drives the loop). The nil path uses a buffered-1 channel
// for consistency with the non-nil path so any code that hand-sends
// into w.C on a nil-registry waiter (test stubs only, in practice)
// coalesces the same way instead of deadlocking.
func (reg *keyWaiterRegistry) Register(keys [][]byte) (*keyWaiter, func()) {
	if reg == nil {
		return &keyWaiter{C: make(chan struct{}, 1)}, func() {}
	}
	w := &keyWaiter{
		C:    make(chan struct{}, 1),
		keys: dedupKeyWaiterKeys(keys),
	}
	reg.mu.Lock()
	for _, s := range w.keys {
		set := reg.waiters[s]
		if set == nil {
			set = map[*keyWaiter]struct{}{}
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

func (reg *keyWaiterRegistry) unregister(w *keyWaiter) {
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

// Signal wakes every waiter registered for key. Caller is the local
// write command (ZADD / ZINCRBY) after coordinator.Dispatch returns;
// by that point the FSM has applied the entry and a re-running
// "try" helper on the leader is guaranteed to see it. Nil-safe:
// test stubs that construct a RedisServer literal directly may
// leave the registry unset, in which case Signal is a no-op.
func (reg *keyWaiterRegistry) Signal(key []byte) {
	if reg == nil {
		return
	}
	reg.mu.Lock()
	// staticcheck SA6001: indexing the map directly with the byte
	// conversion lets the compiler skip the temporary string
	// allocation on the lookup-only path.
	set := reg.waiters[string(key)]
	if len(set) == 0 {
		reg.mu.Unlock()
		return
	}
	waiters := make([]*keyWaiter, 0, len(set))
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

// dedupKeyWaiterKeys returns a new slice of stringified keys with
// duplicates removed, preserving first-seen order. Empty input
// returns an empty, non-nil slice so the caller can range over it
// without a nil-guard.
func dedupKeyWaiterKeys(keys [][]byte) []string {
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
