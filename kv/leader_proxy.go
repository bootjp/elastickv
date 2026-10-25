package kv

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

const leaderForwardTimeout = 5 * time.Second
const maxForwardRetries = 3

// leaderProxyRetryBudget bounds how long forwardWithRetry keeps polling
// for a leader address while none is published yet. gRPC callers expect
// linearizable semantics, so the proxy hides brief re-election windows
// behind a bounded retry instead of returning ErrLeaderNotFound on the
// very first attempt.
const leaderProxyRetryBudget = 5 * time.Second

// leaderProxyRetryInterval paces re-resolution of the leader address.
const leaderProxyRetryInterval = 25 * time.Millisecond

// ErrTransactionOutcomeUnknown means a forwarded mutation may have reached the
// leader, but its result was lost. The same logical mutation must not be sent
// again without a durable operation identity and an authoritative resolution.
var ErrTransactionOutcomeUnknown = errors.New("transaction outcome unknown")

// Only the leader's structured rejection certifies a definitive OCC conflict.
func isDefinitiveForwardConflict(err error) bool {
	return IsForwardWriteConflict(err)
}

// LeaderProxy forwards transactional requests to the current raft leader when
// the local node is not the leader.
type LeaderProxy struct {
	engine raftengine.Engine
	tm     *TransactionManager
	group  *ShardGroup

	connCache      GRPCConnCache
	forwardBreaker leaderProxyCircuitBreaker
	forwardSeq     atomic.Uint64
}

func NewLeaderProxyWithEngine(engine raftengine.Engine, opts ...TransactionOption) *LeaderProxy {
	return &LeaderProxy{
		engine: engine,
		tm:     NewTransactionWithProposer(engine, opts...),
	}
}

func (p *LeaderProxy) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	return p.commitWithLocalStamp(ctx, reqs, nil)
}

// commitWithLocalStamp runs the shard coordinator's leader-issued timestamp
// hook only after the proxy has selected and verified the local leader path.
// A follower can become leader after the coordinator chose to forward a seal.
func (p *LeaderProxy) commitWithLocalStamp(ctx context.Context, reqs []*pb.Request, stamp func(context.Context, []*pb.Request) (uint64, error)) (*TransactionResponse, error) {
	if !isLeaderEngine(p.engine) {
		return p.forwardWithRetry(ctx, reqs)
	}
	// Verify leadership with a quorum to avoid accepting writes on a stale leader.
	// The caller's ctx (via verifyLeaderEngineCtx) bounds the ReadIndex
	// round-trip; verifyLeaderEngine's no-arg variant remains as the
	// background-caller fallback (#745) but is no longer hit on the
	// dispatch hot path.
	if err := verifyLeaderEngineCtx(ctx, p.engine); err != nil {
		return p.forwardWithRetry(ctx, reqs)
	}
	p.forwardBreaker.reset(leaderProxyIdentityFromEngine(p.engine))
	var commitTS uint64
	if stamp != nil {
		var err error
		commitTS, err = stamp(ctx, reqs)
		if err != nil {
			return nil, err
		}
	}
	resp, err := p.tm.Commit(ctx, reqs)
	if err != nil || resp == nil || commitTS == 0 {
		return resp, err
	}
	if resp.CommitTS != 0 && resp.CommitTS != commitTS {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	resp.CommitTS = commitTS
	return resp, nil
}

func (p *LeaderProxy) Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	if !isLeaderEngine(p.engine) {
		return p.forwardWithRetry(ctx, reqs)
	}
	// Verify leadership with a quorum to avoid accepting aborts on a stale leader.
	if err := verifyLeaderEngineCtx(ctx, p.engine); err != nil {
		return p.forwardWithRetry(ctx, reqs)
	}
	p.forwardBreaker.reset(leaderProxyIdentityFromEngine(p.engine))
	return p.tm.Abort(ctx, reqs)
}

// forwardWithRetry attempts to forward to the leader, re-fetching the
// leader address on each failure to handle leadership changes between
// attempts. Two retry signals are interleaved:
//
//   - Forward-RPC failures are bounded by maxForwardRetries (each attempt
//     re-resolves the leader address inside forward()).
//   - Transient leader-unavailable errors (no leader published yet, or
//     the forwarded RPC landed on a stale leader that returned a typed
//     pre-submission rejection) are bounded by
//     leaderProxyRetryBudget so a brief re-election window does not
//     bubble up to gRPC clients as a hard failure. Linearizable callers
//     expect the proxy to either commit atomically or fail definitively,
//     not to leak transient raft-internal churn.
//
// The forwarding endpoint marks pre-submission rejection in structured gRPC
// details so a stale leader can be retried without inspecting display text.
//
// The overall budget is strictly enforced: no new forward() attempt is
// started once time.Now() has passed deadline, and each per-attempt RPC
// is bounded by min(leaderForwardTimeout, remaining budget). Without
// that second bound, a single forward() could run for the full 5s RPC
// timeout AFTER the budget expired, pushing total latency well past
// leaderProxyRetryBudget.
func (p *LeaderProxy) forwardWithRetry(callerCtx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}

	deadline := time.Now().Add(leaderProxyRetryBudget)
	// Parent context carries the retry deadline so forward()'s per-call
	// timeout (derived via context.WithTimeout(parentCtx, ...)) can
	// never extend past it — context.WithTimeout picks the earlier of
	// the two expirations. callerCtx is the dispatch handler's own
	// context, so a Redis client whose deadline expires before the
	// retry budget exits early without waiting out the full 5 s.
	parentCtx, cancelParent := context.WithDeadline(callerCtx, deadline)
	defer cancelParent()
	requestID := p.forwardSeq.Add(1)
	defer p.forwardBreaker.releaseRequest(requestID)

	var lastErr error
	for {
		// runForwardCycle runs up to maxForwardRetries fast retries against
		// whatever leader is currently visible and returns either a
		// committed response, a terminal error, or the last transient
		// leader error for the outer loop to re-poll on.
		resp, err, done := p.runForwardCycle(callerCtx, parentCtx, reqs, deadline, requestID)
		if done {
			return resp, err
		}
		if !errors.Is(err, ErrLeaderProxyCircuitOpen) {
			lastErr = err
		}
		// Defensive: if runForwardCycle exited on the deadline guard
		// before ever calling forward() (e.g. a future refactor
		// shortens the budget, or the clock jumps forward between the
		// outer deadline computation and the inner check), lastErr
		// stays nil. errors.Wrapf(nil, ...) would silently yield nil
		// — handing callers a (nil, nil) "success" that never
		// happened. Surface a real error instead.
		if lastErr == nil {
			return nil, errors.WithStack(ErrLeaderNotFound)
		}
		if !time.Now().Before(deadline) {
			return nil, lastErr
		}
		waitLeaderProxyBackoff(parentCtx, leaderProxyRetryInterval, deadline)
		if err := callerCtx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}
		// Re-check the deadline AFTER the back-off: if the budget is
		// exhausted, do not enter another maxForwardRetries cycle
		// (which could issue up to three more RPCs, each bounded by
		// leaderForwardTimeout relative to the now-expired deadline).
		if !time.Now().Before(deadline) {
			return nil, lastErr
		}
	}
}

// waitLeaderProxyBackoff sleeps for up to interval but never past the
// remaining budget, and is interruptible via parentCtx — so a parent
// deadline or cancellation tears the back-off down immediately instead
// of waiting out the full interval. Factored out of forwardWithRetry
// so that function stays under the cyclop threshold.
func waitLeaderProxyBackoff(parentCtx context.Context, interval time.Duration, deadline time.Time) {
	sleep := interval
	if until := time.Until(deadline); until > 0 && until < sleep {
		sleep = until
	}
	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-parentCtx.Done():
	case <-timer.C:
	}
}

// runForwardCycle issues up to maxForwardRetries forward() attempts
// against the current leader, short-circuiting on the budget.
// Returns:
//   - (resp, nil, true) on a committed success — caller should return it.
//   - (nil, err, true) on a non-transient error wrapped with the
//     retry-count context — caller should return it.
//   - (nil, lastTransientErr, false) when every attempt failed with a
//     transient leader-unavailable signal — caller should back off and
//     retry the cycle.
//   - (nil, nil, false) when the inner loop exited on the deadline
//     guard before calling forward() at all; caller surfaces
//     ErrLeaderNotFound for that defensive path.
func (p *LeaderProxy) runForwardCycle(callerCtx context.Context, parentCtx context.Context, reqs []*pb.Request, deadline time.Time, requestID uint64) (*TransactionResponse, error, bool) {
	var lastErr error
	for attempt := 0; attempt < maxForwardRetries; attempt++ {
		if err := callerCtx.Err(); err != nil {
			return nil, errors.WithStack(err), true
		}
		if !time.Now().Before(deadline) {
			// Budget expired mid-cycle; do not start another RPC.
			break
		}
		resp, err := p.forward(callerCtx, parentCtx, reqs, requestID)
		if err == nil {
			return resp, nil, true
		}
		lastErr = err
		if isTerminalForwardError(err) {
			return nil, err, true
		}
		if callerErr := callerCtx.Err(); callerErr != nil {
			return nil, errors.WithStack(callerErr), true
		}
		if leaveCycle, done := p.forwardFailureDecision(err, requestID); leaveCycle {
			return nil, err, done
		}
	}
	if lastErr != nil && !isTransientLeaderError(lastErr) {
		return nil, errors.Wrapf(lastErr, "leader forward failed after %d retries", maxForwardRetries), true
	}
	return nil, lastErr, false
}

func isTerminalForwardError(err error) bool {
	return errors.Is(err, ErrTransactionOutcomeUnknown) ||
		errors.Is(err, store.ErrWriteConflict) ||
		errors.Is(err, ErrTxnLocked) ||
		errors.Is(err, ErrComposed1Violation) ||
		errors.Is(err, ErrComposed1VersionGCd)
}

func (p *LeaderProxy) forwardFailureDecision(err error, requestID uint64) (leaveCycle bool, done bool) {
	if errors.Is(err, ErrLeaderProxyCircuitOpen) {
		identity := leaderProxyIdentityFromEngine(p.engine)
		return true, !p.forwardBreaker.mayRetryAfterOpen(identity, requestID)
	}
	if isTransientLeaderError(err) {
		return true, false
	}
	if !isLeaderProxyBreakerFailure(err) {
		return false, false
	}
	identity := leaderProxyIdentityFromEngine(p.engine)
	if p.forwardBreaker.mayRetryAfterOpen(identity, requestID) {
		return true, false
	}
	return false, false
}

func (p *LeaderProxy) forward(callerCtx context.Context, parentCtx context.Context, reqs []*pb.Request, requestID uint64) (*TransactionResponse, error) {
	if err := callerCtx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	identity := leaderProxyIdentityFromEngine(p.engine)
	if err := p.forwardBreaker.allow(identity, requestID, time.Now()); err != nil {
		return nil, err
	}
	addr := identity.address
	if addr == "" {
		return nil, p.recordForwardFailure(callerCtx, identity, requestID, ErrLeaderNotFound)
	}

	conn, err := p.connCache.ConnFor(addr)
	if err != nil {
		return nil, p.recordForwardFailure(callerCtx, identity, requestID, err)
	}

	cli := pb.NewInternalClient(conn)
	// context.WithTimeout on a deadline-bounded parent yields the
	// earlier of the two — so a forward() issued with <5s of budget
	// remaining caps at exactly the budget, not the full RPC timeout.
	ctx, cancel := context.WithTimeout(parentCtx, leaderForwardTimeout)
	defer cancel()

	resp, err := cli.Forward(ctx, &pb.ForwardRequest{
		IsTxn:    reqs[0].IsTxn,
		Requests: reqs,
	})
	if err != nil {
		// Preserve the breaker accounting, but caller cancellation is not
		// evidence that the remote mutation did not commit.
		_ = p.recordForwardFailure(callerCtx, identity, requestID, err)
		return nil, classifyForwardRPCError(err, reqs)
	}
	p.forwardBreaker.record(identity, requestID, nil, time.Now())
	if !resp.Success {
		return nil, fmt.Errorf("%w: forward response lacks a decision", ErrTransactionOutcomeUnknown)
	}
	return &TransactionResponse{CommitIndex: resp.CommitIndex, CommitTS: resp.CommitTs}, nil
}

func classifyForwardRPCError(err error, reqs []*pb.Request) error {
	if isDefinitiveForwardNotLeader(err) {
		return errors.WithStack(err)
	}
	if len(reqs) == 1 && reqs[0] != nil &&
		(reqs[0].Phase == pb.Phase_NONE || reqs[0].Phase == pb.Phase_PREPARE) {
		switch {
		case isDefinitiveForwardConflict(err):
			return fmt.Errorf("%w: %w", store.ErrWriteConflict, err)
		case IsForwardTxnLocked(err):
			return fmt.Errorf("%w: %w", ErrTxnLocked, err)
		case IsForwardComposed1Violation(err):
			return fmt.Errorf("%w: %w", ErrComposed1Violation, err)
		case IsForwardComposed1VersionGCd(err):
			return fmt.Errorf("%w: %w", ErrComposed1VersionGCd, err)
		}
	}
	return fmt.Errorf("%w: %w", ErrTransactionOutcomeUnknown, err)
}

func isDefinitiveForwardNotLeader(err error) bool {
	return IsForwardNotLeader(err)
}

func (p *LeaderProxy) forwardLeaseRead(callerCtx context.Context) (leaseReadResult, error) {
	deadline := time.Now().Add(leaderProxyRetryBudget)
	ctx, cancel := context.WithDeadline(callerCtx, deadline)
	defer cancel()
	var lastErr error
	for time.Now().Before(deadline) {
		result, err := p.forwardLeaseReadOnce(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !isTransientLeaderError(err) {
			return leaseReadResult{}, err
		}
		waitLeaderProxyBackoff(ctx, leaderProxyRetryInterval, deadline)
	}
	if lastErr == nil {
		lastErr = ErrLeaderNotFound
	}
	return leaseReadResult{}, errors.WithStack(lastErr)
}

func (p *LeaderProxy) forwardLeaseReadOnce(parentCtx context.Context) (leaseReadResult, error) {
	addr := leaderAddrFromEngine(p.engine)
	if addr == "" {
		return leaseReadResult{}, errors.WithStack(ErrLeaderNotFound)
	}
	conn, err := p.connCache.ConnFor(addr)
	if err != nil {
		return leaseReadResult{}, err
	}
	ctx, cancel := context.WithTimeout(parentCtx, leaderForwardTimeout)
	defer cancel()
	if token := p.group.forwardedLeaderReadToken(); token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	resp, err := pb.NewInternalClient(conn).ForwardLeaseRead(ctx, &pb.ForwardLeaseReadRequest{})
	if err != nil {
		return leaseReadResult{}, errors.WithStack(err)
	}
	return leaseReadResult{appliedIndex: resp.GetAppliedIndex(), lastCommitTS: resp.GetLastCommitTs()}, nil
}

func (p *LeaderProxy) recordForwardFailure(callerCtx context.Context, identity leaderProxyIdentity, requestID uint64, err error) error {
	if callerErr := callerCtx.Err(); callerErr != nil {
		p.forwardBreaker.release(identity, requestID)
		return errors.WithStack(callerErr)
	}
	wrapped := errors.WithStack(err)
	p.forwardBreaker.record(identity, requestID, wrapped, time.Now())
	return wrapped
}

var _ Transactional = (*LeaderProxy)(nil)
var _ io.Closer = (*LeaderProxy)(nil)

func (p *LeaderProxy) Close() error {
	if p == nil {
		return nil
	}
	return p.connCache.Close()
}
