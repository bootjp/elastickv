package kv

import (
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	leaderProxyBreakerFailureThreshold = 3
	leaderProxyBreakerBaseBackoff      = 100 * time.Millisecond
	leaderProxyBreakerMaxBackoff       = 2 * time.Second
)

// ErrLeaderProxyCircuitOpen indicates that recent attempts against the same
// leader identity failed and the shared forwarding breaker is suppressing a
// retry storm. It is transient: callers may retry after leader publication or
// the breaker backoff, but a single request must not busy-loop on it.
var ErrLeaderProxyCircuitOpen = errors.New("leader proxy circuit open")

type leaderProxyIdentity struct {
	id      string
	address string
	term    uint64
}

func leaderProxyIdentityFromEngine(engine raftengine.Engine) leaderProxyIdentity {
	if engine == nil {
		return leaderProxyIdentity{}
	}
	status := engine.Status()
	leader := status.Leader
	if leader.ID == "" && leader.Address == "" {
		leader = engine.Leader()
	}
	return leaderProxyIdentity{id: leader.ID, address: leader.Address, term: status.Term}
}

type leaderProxyCircuitBreaker struct {
	mu sync.Mutex

	identity      leaderProxyIdentity
	initialized   bool
	failures      int
	openUntil     time.Time
	openUntilText string
	probeInFlight bool
	owner         uint64
}

func (b *leaderProxyCircuitBreaker) allow(identity leaderProxyIdentity, requestID uint64, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resetForIdentityLocked(identity)
	if now.Before(b.openUntil) {
		return leaderProxyCircuitOpenError(identity, b.openUntilText)
	}
	if b.openUntil.IsZero() {
		return nil
	}
	if b.owner != 0 && b.owner != requestID {
		return leaderProxyCircuitOpenError(identity, b.openUntilText)
	}
	if b.probeInFlight {
		return leaderProxyCircuitOpenError(identity, b.openUntilText)
	}
	b.probeInFlight = true
	if b.owner == 0 {
		b.owner = requestID
	}
	return nil
}

func (b *leaderProxyCircuitBreaker) record(identity leaderProxyIdentity, requestID uint64, err error, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Every record follows a successful allow. If the identity has changed in
	// the meantime, this is a late result from the old leader and must not
	// resurrect stale breaker state.
	if !b.initialized || b.identity != identity {
		return
	}
	if !isLeaderProxyBreakerFailure(err) {
		b.failures = 0
		b.openUntil = time.Time{}
		b.openUntilText = ""
		b.probeInFlight = false
		b.owner = 0
		return
	}
	if !b.openUntil.IsZero() && b.owner != requestID {
		return
	}
	b.probeInFlight = false
	b.failures++
	if b.failures < leaderProxyBreakerFailureThreshold {
		return
	}
	b.openUntil = now.Add(leaderProxyBreakerBackoff(b.failures))
	b.openUntilText = b.openUntil.UTC().Format(time.RFC3339Nano)
	b.owner = requestID
}

// release abandons recovery ownership without treating caller cancellation as
// evidence that the leader is unhealthy. The next request may take the probe.
func (b *leaderProxyCircuitBreaker) release(identity leaderProxyIdentity, requestID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.initialized || b.identity != identity || b.owner != requestID {
		return
	}
	b.probeInFlight = false
	b.owner = 0
}

func (b *leaderProxyCircuitBreaker) releaseRequest(requestID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.owner != requestID {
		return
	}
	b.probeInFlight = false
	b.owner = 0
}

func (b *leaderProxyCircuitBreaker) owns(identity leaderProxyIdentity, requestID uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.initialized && b.identity == identity && b.owner == requestID
}

func (b *leaderProxyCircuitBreaker) mayRetryAfterOpen(current leaderProxyIdentity, requestID uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.initialized && (b.identity != current || b.owner == requestID)
}

func (b *leaderProxyCircuitBreaker) reset(identity leaderProxyIdentity) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.identity = identity
	b.initialized = true
	b.failures = 0
	b.openUntil = time.Time{}
	b.openUntilText = ""
	b.probeInFlight = false
	b.owner = 0
}

func (b *leaderProxyCircuitBreaker) resetForIdentityLocked(identity leaderProxyIdentity) {
	if b.initialized && b.identity == identity {
		return
	}
	b.identity = identity
	b.initialized = true
	b.failures = 0
	b.openUntil = time.Time{}
	b.openUntilText = ""
	b.probeInFlight = false
	b.owner = 0
}

func leaderProxyCircuitOpenError(identity leaderProxyIdentity, untilText string) error {
	return errors.Wrapf(ErrLeaderProxyCircuitOpen,
		"leader id=%q address=%q term=%d unavailable until %s",
		identity.id, identity.address, identity.term, untilText)
}

func leaderProxyBreakerBackoff(failures int) time.Duration {
	exponent := failures - leaderProxyBreakerFailureThreshold
	backoff := leaderProxyBreakerBaseBackoff
	for range exponent {
		if backoff >= leaderProxyBreakerMaxBackoff/2 {
			return leaderProxyBreakerMaxBackoff
		}
		backoff *= 2
	}
	return min(backoff, leaderProxyBreakerMaxBackoff)
}

func isLeaderProxyBreakerFailure(err error) bool {
	if err == nil {
		return false
	}
	if isTransientLeaderError(err) {
		return true
	}
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}
