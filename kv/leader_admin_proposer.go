package kv

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type LeaderAdminProposerOption func(*leaderAdminProposer)

func WithLeaderAdminToken(token string) LeaderAdminProposerOption {
	return func(p *leaderAdminProposer) {
		p.adminToken = token
	}
}

// leaderAdminProposer forwards ProposeAdmin calls to the current group leader.
// Propose remains local because only barrier-exempt, idempotent admin entries
// are safe to retry across a leadership change.
type leaderAdminProposer struct {
	leader     raftengine.LeaderView
	local      raftengine.Proposer
	connCache  *GRPCConnCache
	adminToken string
}

func NewLeaderAdminProposer(
	leader raftengine.LeaderView,
	local raftengine.Proposer,
	connCache *GRPCConnCache,
	opts ...LeaderAdminProposerOption,
) raftengine.Proposer {
	p := &leaderAdminProposer{leader: leader, local: local, connCache: connCache}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

func (p *leaderAdminProposer) Propose(
	ctx context.Context,
	data []byte,
) (*raftengine.ProposalResult, error) {
	if p.local == nil {
		return nil, errors.New("local proposer is unavailable")
	}
	result, err := p.local.Propose(ctx, data)
	return result, errors.WithStack(err)
}

func (p *leaderAdminProposer) ProposeAdmin(
	ctx context.Context,
	data []byte,
) (*raftengine.ProposalResult, error) {
	if p.local == nil || p.leader == nil {
		return nil, errors.New("leader admin proposer is unavailable")
	}
	if p.leader.State() == raftengine.StateLeader {
		if err := p.leader.VerifyLeader(ctx); err == nil {
			result, err := p.local.ProposeAdmin(ctx, data)
			if err == nil || !isTransientLeaderError(err) {
				return result, errors.WithStack(err)
			}
		}
	}
	return p.forwardAdminWithRetry(ctx, data)
}

func (p *leaderAdminProposer) forwardAdminWithRetry(
	callerCtx context.Context,
	data []byte,
) (*raftengine.ProposalResult, error) {
	deadline := time.Now().Add(leaderProxyRetryBudget)
	parentCtx, cancel := context.WithDeadline(callerCtx, deadline)
	defer cancel()

	var lastErr error
	for {
		result, err, done := p.runAdminForwardCycle(parentCtx, data, deadline)
		if done {
			return result, err
		}
		lastErr = err
		if lastErr == nil {
			return nil, errors.WithStack(ErrLeaderNotFound)
		}
		if !time.Now().Before(deadline) {
			return nil, lastErr
		}
		waitLeaderProxyBackoff(parentCtx, leaderProxyRetryInterval, deadline)
		if !time.Now().Before(deadline) {
			return nil, lastErr
		}
	}
}

func (p *leaderAdminProposer) runAdminForwardCycle(
	ctx context.Context,
	data []byte,
	deadline time.Time,
) (*raftengine.ProposalResult, error, bool) {
	var lastErr error
	for attempt := 0; attempt < maxForwardRetries; attempt++ {
		if !time.Now().Before(deadline) {
			break
		}
		result, err := p.forwardAdmin(ctx, data)
		if err == nil {
			return result, nil, true
		}
		lastErr = err
		if isTransientLeaderError(err) {
			break
		}
	}
	if lastErr != nil && !isTransientLeaderError(lastErr) {
		return nil, errors.Wrapf(lastErr, "leader admin proposal failed after %d retries", maxForwardRetries), true
	}
	return nil, lastErr, false
}

func (p *leaderAdminProposer) forwardAdmin(
	parentCtx context.Context,
	data []byte,
) (*raftengine.ProposalResult, error) {
	addr := leaderAddrFromEngine(p.leader)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}
	if p.connCache == nil {
		return nil, errors.New("leader admin proposer connection cache is unavailable")
	}
	conn, err := p.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parentCtx, leaderForwardTimeout)
	defer cancel()
	if p.adminToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.adminToken)
	}
	resp, err := pb.NewInternalClient(conn).ForwardAdminProposal(ctx, &pb.ForwardAdminProposalRequest{Payload: data})
	if err != nil {
		if code := status.Code(err); code != codes.Unknown {
			return nil, status.Errorf(code, "forward admin proposal: %v", err)
		}
		return nil, errors.Wrap(err, "forward admin proposal")
	}
	return &raftengine.ProposalResult{CommitIndex: resp.GetCommitIndex()}, nil
}

var _ raftengine.Proposer = (*leaderAdminProposer)(nil)
