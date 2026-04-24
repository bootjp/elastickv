package admin

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// rateLimiterMaxEntries is the point at which the limiter performs a
// sweep of stale entries. Tuned so a reasonable burst of distinct
// source IPs does not force a pathological cleanup on every login.
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

	// Opportunistic cleanup: if the map ever exceeds a threshold, walk
	// entries and drop stale windows. Keeps memory bounded against a
	// spray of distinct source IPs.
	if len(rl.entries) > rateLimiterMaxEntries {
		for k, v := range rl.entries {
			if now.Sub(v.windowStart) > rl.window {
				delete(rl.entries, k)
			}
		}
	}

	e, ok := rl.entries[ip]
	if !ok || now.Sub(e.windowStart) >= rl.window {
		rl.entries[ip] = &rateLimiterEntry{windowStart: now, count: 1}
		return true
	}
	if e.count >= rl.limit {
		return false
	}
	e.count++
	return true
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
