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

	i.stampTimestamps(req)

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

func (i *Internal) stampTimestamps(req *pb.ForwardRequest) {
	if req == nil {
		return
	}
	if req.IsTxn {
		var startTs uint64
		// All requests in a transaction must have the same timestamp.
		// Find a timestamp from the requests, or generate a new one if none exist.
		for _, r := range req.Requests {
			if r.Ts != 0 {
				startTs = r.Ts
				break
			}
		}

		if startTs == 0 && len(req.Requests) > 0 {
			startTs = i.clock.Next()
		}

		// Assign the unified timestamp to all requests in the transaction.
		for _, r := range req.Requests {
			r.Ts = startTs
		}
		return
	}

	for _, r := range req.Requests {
		if r.Ts == 0 {
			r.Ts = i.clock.Next()
		}
	}
}
