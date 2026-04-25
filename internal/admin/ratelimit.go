package admin

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
)

// rateLimiterMaxEntries is a hard cap on the number of distinct source
// IPs the limiter will track at once. Hitting the cap means an attacker
// is spraying us with unique source addresses; we respond by refusing
// to add new entries (and therefore refusing those logins) until the
// window ages them out. We first sweep expired windows before
// concluding the map is full, so well-behaved traffic never trips on
// the cap.
const rateLimiterMaxEntries = 1024

// ipv4PrefixBits / ipv6PrefixBits are the host-route prefix lengths
// used when ParseTrustedProxies receives a bare IP literal.
const (
	ipv4PrefixBits = 32
	ipv6PrefixBits = 128
)

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

// clientIP extracts the IP part of the request's remote address. It
// falls back to the full RemoteAddr if SplitHostPort fails. By default
// we do NOT consult X-Forwarded-For because honouring it for arbitrary
// clients would let attackers evade the per-IP rate limiter; trusted
// proxy support is added by clientIPWithTrust below for deployments
// behind a known load balancer.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// clientIPWithTrust extends clientIP for deployments behind a trusted
// load balancer. If the immediate peer address is one of the trusted
// proxies, X-Forwarded-For is parsed and the rightmost address that
// is NOT itself a trusted proxy is returned (this is the closest hop
// to the actual client we can authenticate). Untrusted peers fall
// straight through to clientIP, so a hostile client cannot spoof
// X-Forwarded-For unless it managed to come from inside the trust
// boundary in the first place.
func clientIPWithTrust(r *http.Request, trusted []*net.IPNet) string {
	peer := clientIP(r)
	if len(trusted) == 0 || !ipMatchesAny(peer, trusted) {
		return peer
	}
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return peer
	}
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(parts[i])
		if candidate == "" {
			continue
		}
		if ipMatchesAny(candidate, trusted) {
			continue
		}
		return candidate
	}
	return peer
}

func ipMatchesAny(ip string, nets []*net.IPNet) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

// ParseTrustedProxies converts a list of CIDRs and bare IP literals into
// *net.IPNets, accepting both forms operators commonly write in config
// files. A bare IP is treated as a /32 (or /128 for IPv6) network. The
// function is exported so the wiring layer can pre-validate operator
// input at startup rather than discovering it on the first login.
func ParseTrustedProxies(specs []string) ([]*net.IPNet, error) {
	out := make([]*net.IPNet, 0, len(specs))
	for _, s := range specs {
		trim := strings.TrimSpace(s)
		if trim == "" {
			continue
		}
		if _, cidr, err := net.ParseCIDR(trim); err == nil {
			out = append(out, cidr)
			continue
		}
		ip := net.ParseIP(trim)
		if ip == nil {
			return nil, errors.WithStack(errors.Newf("trusted proxy %q is not a valid IP or CIDR", trim))
		}
		mask := net.CIDRMask(ipv4PrefixBits, ipv4PrefixBits)
		if ip.To4() == nil {
			mask = net.CIDRMask(ipv6PrefixBits, ipv6PrefixBits)
		}
		out = append(out, &net.IPNet{IP: ip, Mask: mask})
	}
	return out, nil
}
