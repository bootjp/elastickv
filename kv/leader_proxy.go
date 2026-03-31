package kv

import (
	"context"
	"io"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const leaderForwardTimeout = 5 * time.Second
const maxForwardRetries = 3

// LeaderProxy forwards transactional requests to the current raft leader when
// the local node is not the leader.
type LeaderProxy struct {
	raft *raft.Raft
	tm   *TransactionManager

	connCache GRPCConnCache
}

// NewLeaderProxy creates a leader-aware transactional proxy for a raft group.
func NewLeaderProxy(r *raft.Raft, opts ...TransactionOption) *LeaderProxy {
	return &LeaderProxy{
		raft: r,
		tm:   NewTransaction(r, opts...),
	}
}

func (p *LeaderProxy) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	if p.raft.State() != raft.Leader {
		return p.forwardWithRetry(reqs)
	}
	// Verify leadership with a quorum to avoid accepting writes on a stale leader.
	if err := verifyRaftLeader(p.raft); err != nil {
		return p.forwardWithRetry(reqs)
	}
	return p.tm.Commit(reqs)
}

func (p *LeaderProxy) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	if p.raft.State() != raft.Leader {
		return p.forwardWithRetry(reqs)
	}
	// Verify leadership with a quorum to avoid accepting aborts on a stale leader.
	if err := verifyRaftLeader(p.raft); err != nil {
		return p.forwardWithRetry(reqs)
	}
	return p.tm.Abort(reqs)
}

// forwardWithRetry attempts to forward to the leader up to maxForwardRetries
// times, re-fetching the leader address on each failure to handle leadership
// changes between attempts.
func (p *LeaderProxy) forwardWithRetry(reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}

	var lastErr error
	for attempt := 0; attempt < maxForwardRetries; attempt++ {
		resp, err := p.forward(reqs)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		// If the leader is simply not found, retry won't help immediately.
		if errors.Is(err, ErrLeaderNotFound) {
			return nil, err
		}
	}
	return nil, errors.Wrapf(lastErr, "leader forward failed after %d retries", maxForwardRetries)
}

func (p *LeaderProxy) forward(reqs []*pb.Request) (*TransactionResponse, error) {
	addr, _ := p.raft.LeaderWithID()
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
