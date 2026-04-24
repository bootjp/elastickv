package admin

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// rateLimiterMaxEntries is a hard cap on the number of distinct source
// IPs the limiter will track at once. Hitting the cap means an attacker
// is spraying us with unique source addresses; we respond by refusing
// to add new entries (and therefore refusing those logins) until the
// window ages them out. We first sweep expired windows before
// concluding the map is full, so well-behaved traffic never trips on
// the cap.
const rateLimiterMaxEntries = 1024

// rateLimiter is a fixed-window, in-memory per-IP rate limiter. It is
// intentionally simple: the admin login endpoint is low-volume, we only
// need to slow brute-force guessing, and distributed accounting would
// require Raft round-trips per login (unreasonable for the threat model).
// Entries older than window are pruned lazily on the next hit.
type rateLimiter struct {
	limit  int
	window time.Duration
	clock  Clock

	mu      sync.Mutex
	entries map[string]*rateLimiterEntry
}

type rateLimiterEntry struct {
	windowStart time.Time
	count       int
}

func newRateLimiter(limit int, window time.Duration, clock Clock) *rateLimiter {
	if clock == nil {
		clock = SystemClock
	}
	return &rateLimiter{
		limit:   limit,
		window:  window,
		clock:   clock,
		entries: make(map[string]*rateLimiterEntry),
	}
}

// allow returns true if the client at ip may perform one more action
// within the current window, otherwise false. It is safe for concurrent
// use.
func (rl *rateLimiter) allow(ip string) bool {
	now := rl.clock().UTC()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	e, ok := rl.entries[ip]
	if ok {
		if now.Sub(e.windowStart) >= rl.window {
			e.windowStart = now
			e.count = 1
			return true
		}
		if e.count >= rl.limit {
			return false
		}
		e.count++
		return true
	}

	// Unknown IP — we need to create a new entry. Enforce the hard
	// cap on distinct tracked IPs before doing so.
	if len(rl.entries) >= rateLimiterMaxEntries {
		// Try to reclaim space from expired windows first. If that
		// still leaves us at cap, refuse the new entry. Refusing
		// (rather than evicting an arbitrary old entry) is safer:
		// it prevents a spray of fresh IPs from silently erasing a
		// legitimate user's in-progress rate-limit state.
		rl.sweepExpiredLocked(now)
		if len(rl.entries) >= rateLimiterMaxEntries {
			return false
		}
	}
	rl.entries[ip] = &rateLimiterEntry{windowStart: now, count: 1}
	return true
}

// sweepExpiredLocked drops entries whose window has elapsed. The caller
// must hold rl.mu.
func (rl *rateLimiter) sweepExpiredLocked(now time.Time) {
	for k, v := range rl.entries {
		if now.Sub(v.windowStart) >= rl.window {
			delete(rl.entries, k)
		}
	}
}

// clientIP extracts the IP part of the request's remote address. It falls
// back to the full RemoteAddr if SplitHostPort fails. We do not consult
// X-Forwarded-For here because the admin listener is expected to run
// directly on the node (loopback or behind a trusted load balancer in
// the TLS case); honouring client-controlled headers would let an
// attacker evade the limiter.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
