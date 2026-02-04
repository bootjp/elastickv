package kv

import (
	"context"

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
}

// NewLeaderProxy creates a leader-aware transactional proxy for a raft group.
func NewLeaderProxy(r *raft.Raft) *LeaderProxy {
	return &LeaderProxy{
		raft: r,
		tm:   NewTransaction(r),
	}
}

func (p *LeaderProxy) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	if p.raft.State() == raft.Leader {
		return p.tm.Commit(reqs)
	}
	return p.forward(reqs)
}

func (p *LeaderProxy) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	if p.raft.State() == raft.Leader {
		return p.tm.Abort(reqs)
	}
	return p.forward(reqs)
}

func (p *LeaderProxy) forward(reqs []*pb.Request) (*TransactionResponse, error) {
	if len(reqs) == 0 {
		return &TransactionResponse{}, nil
	}
	addr, _ := p.raft.LeaderWithID()
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

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

var _ Transactional = (*LeaderProxy)(nil)
