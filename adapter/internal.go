package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

func NewInternal(txm kv.Transactional, r *raft.Raft, clock *kv.HLC) *Internal {
	return &Internal{
		raft:               r,
		transactionManager: txm,
		clock:              clock,
	}
}

type Internal struct {
	raft               *raft.Raft
	transactionManager kv.Transactional
	clock              *kv.HLC

	pb.UnimplementedInternalServer
}

var _ pb.InternalServer = (*Internal)(nil)

var ErrNotLeader = errors.New("not leader")
var ErrLeaderNotFound = errors.New("leader not found")

func (i *Internal) Forward(_ context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if i.raft.State() != raft.Leader {
		return nil, errors.WithStack(ErrNotLeader)
	}

	// Ensure leader issues start_ts when followers forward txn groups without it.
	if req.IsTxn {
		for _, r := range req.Requests {
			if r.Ts == 0 {
				r.Ts = i.clock.Next()
			}
		}
	}

	r, err := i.transactionManager.Commit(req.Requests)
	if err != nil {
		return &pb.ForwardResponse{
			Success:     false,
			CommitIndex: 0,
		}, errors.WithStack(err)
	}

	return &pb.ForwardResponse{
		Success:     true,
		CommitIndex: r.CommitIndex,
	}, nil
}
