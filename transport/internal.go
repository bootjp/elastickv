package transport

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

func NewInternal(txm kv.Transactional, r *raft.Raft) *Internal {
	return &Internal{
		raft:               r,
		transactionManager: txm,
	}
}

type Internal struct {
	raft               *raft.Raft
	transactionManager kv.Transactional

	pb.UnimplementedInternalServer
}

var _ pb.InternalServer = (*Internal)(nil)

var ErrNotLeader = errors.New("not leader")

func (i *Internal) Forward(_ context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if i.raft.State() != raft.Leader {
		return nil, errors.WithStack(ErrNotLeader)
	}

	r, err := i.transactionManager.Commit(req.Requests)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &pb.ForwardResponse{
		Success:     true,
		CommitIndex: r.CommitIndex,
	}, nil
}
