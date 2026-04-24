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
//   - ErrLeaderNotFound (no leader published yet) is bounded by
//     leaderProxyRetryBudget so a brief re-election window does not bubble
//     up to gRPC clients as a hard failure. Linearizable callers expect
//     the proxy to either commit atomically or fail definitively, not to
//     leak transient raft-internal churn.
func (p *LeaderProxy) forwardWithRetry(reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}

	deadline := time.Now().Add(leaderProxyRetryBudget)
	var lastErr error
	for {
		// Each iteration of the outer loop runs up to maxForwardRetries
		// fast retries against whatever leader is currently visible. If
		// none is, we sleep one leaderProxyRetryInterval and re-poll
		// until leaderProxyRetryBudget elapses.
		for attempt := 0; attempt < maxForwardRetries; attempt++ {
			resp, err := p.forward(reqs)
			if err == nil {
				return resp, nil
			}
			lastErr = err
			if errors.Is(err, ErrLeaderNotFound) {
				break
			}
		}
		if !errors.Is(lastErr, ErrLeaderNotFound) {
			return nil, errors.Wrapf(lastErr, "leader forward failed after %d retries", maxForwardRetries)
		}
		if !time.Now().Before(deadline) {
			return nil, lastErr
		}
		time.Sleep(leaderProxyRetryInterval)
	}
}

func (p *LeaderProxy) forward(reqs []*pb.Request) (*TransactionResponse, error) {
	addr := leaderAddrFromEngine(p.engine)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := p.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewInternalClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), leaderForwardTimeout)
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
