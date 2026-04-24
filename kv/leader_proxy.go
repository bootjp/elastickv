package kv

import (
	"context"
	"io"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
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

// LeaderProxy forwards transactional requests to the current raft leader when
// the local node is not the leader.
type LeaderProxy struct {
	engine raftengine.Engine
	tm     *TransactionManager

	connCache GRPCConnCache
}

func NewLeaderProxyWithEngine(engine raftengine.Engine, opts ...TransactionOption) *LeaderProxy {
	return &LeaderProxy{
		engine: engine,
		tm:     NewTransactionWithProposer(engine, opts...),
	}
}

func (p *LeaderProxy) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	if !isLeaderEngine(p.engine) {
		return p.forwardWithRetry(reqs)
	}
	// Verify leadership with a quorum to avoid accepting writes on a stale leader.
	if err := verifyLeaderEngine(p.engine); err != nil {
		return p.forwardWithRetry(reqs)
	}
	return p.tm.Commit(reqs)
}

func (p *LeaderProxy) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	if !isLeaderEngine(p.engine) {
		return p.forwardWithRetry(reqs)
	}
	// Verify leadership with a quorum to avoid accepting aborts on a stale leader.
	if err := verifyLeaderEngine(p.engine); err != nil {
		return p.forwardWithRetry(reqs)
	}
	return p.tm.Abort(reqs)
}

// forwardWithRetry attempts to forward to the leader, re-fetching the
// leader address on each failure to handle leadership changes between
// attempts. Two retry signals are interleaved:
//
//   - Forward-RPC failures are bounded by maxForwardRetries (each attempt
//     re-resolves the leader address inside forward()).
//   - Transient leader-unavailable errors (no leader published yet, or
//     the forwarded RPC landed on a stale leader that returned
//     adapter.ErrNotLeader over the wire) are bounded by
//     leaderProxyRetryBudget so a brief re-election window does not
//     bubble up to gRPC clients as a hard failure. Linearizable callers
//     expect the proxy to either commit atomically or fail definitively,
//     not to leak transient raft-internal churn.
//
// The wire-level "not leader" string match is necessary because a stale
// leader's Internal.Forward returns adapter.ErrNotLeader whose error
// chain does not survive the gRPC boundary; errors.Is against
// ErrLeaderNotFound would miss it and exit after only the fast retries.
//
// The overall budget is strictly enforced: no new forward() attempt is
// started once time.Now() has passed deadline, and each per-attempt RPC
// is bounded by min(leaderForwardTimeout, remaining budget). Without
// that second bound, a single forward() could run for the full 5s RPC
// timeout AFTER the budget expired, pushing total latency well past
// leaderProxyRetryBudget.
func (p *LeaderProxy) forwardWithRetry(reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}

	deadline := time.Now().Add(leaderProxyRetryBudget)
	// Parent context carries the retry deadline so forward()'s per-call
	// timeout (derived via context.WithTimeout(parentCtx, ...)) can
	// never extend past it — context.WithTimeout picks the earlier of
	// the two expirations.
	parentCtx, cancelParent := context.WithDeadline(context.Background(), deadline)
	defer cancelParent()

	var lastErr error
	for {
		// runForwardCycle runs up to maxForwardRetries fast retries against
		// whatever leader is currently visible and returns either a
		// committed response, a terminal error, or the last transient
		// leader error for the outer loop to re-poll on.
		resp, err, done := p.runForwardCycle(parentCtx, reqs, deadline)
		if done {
			return resp, err
		}
		lastErr = err
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
func (p *LeaderProxy) runForwardCycle(parentCtx context.Context, reqs []*pb.Request, deadline time.Time) (*TransactionResponse, error, bool) {
	var lastErr error
	for attempt := 0; attempt < maxForwardRetries; attempt++ {
		if !time.Now().Before(deadline) {
			// Budget expired mid-cycle; do not start another RPC.
			break
		}
		resp, err := p.forward(parentCtx, reqs)
		if err == nil {
			return resp, nil, true
		}
		lastErr = err
		if isTransientLeaderError(err) {
			break
		}
	}
	if lastErr != nil && !isTransientLeaderError(lastErr) {
		return nil, errors.Wrapf(lastErr, "leader forward failed after %d retries", maxForwardRetries), true
	}
	return nil, lastErr, false
}

func (p *LeaderProxy) forward(parentCtx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	addr := leaderAddrFromEngine(p.engine)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := p.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
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
		return nil, errors.WithStack(err)
	}
	if !resp.Success {
		return nil, ErrInvalidRequest
	}
	return &TransactionResponse{CommitIndex: resp.CommitIndex}, nil
}

var _ Transactional = (*LeaderProxy)(nil)
var _ io.Closer = (*LeaderProxy)(nil)

func (p *LeaderProxy) Close() error {
	if p == nil {
		return nil
	}
	return p.connCache.Close()
}
