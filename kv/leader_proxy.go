package kv

import (
	"context"
	"sync"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LeaderProxy forwards transactional requests to the current raft leader when
// the local node is not the leader.
type LeaderProxy struct {
	raft *raft.Raft
	tm   *TransactionManager

	connMu sync.Mutex
	conns  map[raft.ServerAddress]*grpc.ClientConn
}

// NewLeaderProxy creates a leader-aware transactional proxy for a raft group.
func NewLeaderProxy(r *raft.Raft) *LeaderProxy {
	return &LeaderProxy{
		raft: r,
		tm:   NewTransaction(r),
	}
}

func (p *LeaderProxy) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	if p.raft.State() != raft.Leader {
		return p.forward(reqs)
	}
	// Verify leadership with a quorum to avoid accepting writes on a stale leader.
	if err := p.raft.VerifyLeader().Error(); err != nil {
		return p.forward(reqs)
	}
	return p.tm.Commit(reqs)
}

func (p *LeaderProxy) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	if p.raft.State() != raft.Leader {
		return p.forward(reqs)
	}
	// Verify leadership with a quorum to avoid accepting aborts on a stale leader.
	if err := p.raft.VerifyLeader().Error(); err != nil {
		return p.forward(reqs)
	}
	return p.tm.Abort(reqs)
}

func (p *LeaderProxy) forward(reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}
	addr, _ := p.raft.LeaderWithID()
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := p.connFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewInternalClient(conn)
	resp, err := cli.Forward(context.Background(), &pb.ForwardRequest{
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

func (p *LeaderProxy) connFor(addr raft.ServerAddress) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	p.connMu.Lock()
	defer p.connMu.Unlock()

	if p.conns == nil {
		p.conns = make(map[raft.ServerAddress]*grpc.ClientConn)
	}
	if conn, ok := p.conns[addr]; ok && conn != nil {
		return conn, nil
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	p.conns[addr] = conn
	return conn, nil
}

var _ Transactional = (*LeaderProxy)(nil)
