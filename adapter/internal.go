package adapter

import (
	"bytes"
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
		i.stampTxnTimestamps(req.Requests)
		return
	}

	i.stampRawTimestamps(req.Requests)
}

func (i *Internal) stampRawTimestamps(reqs []*pb.Request) {
	for _, r := range reqs {
		if r == nil {
			continue
		}
		if r.Ts != 0 {
			continue
		}
		if i.clock == nil {
			r.Ts = 1
			continue
		}
		r.Ts = i.clock.Next()
	}
}

func (i *Internal) stampTxnTimestamps(reqs []*pb.Request) {
	startTS := forwardedTxnStartTS(reqs)
	if startTS == 0 {
		if i.clock == nil {
			startTS = 1
		} else {
			startTS = i.clock.Next()
		}
	}

	// Assign the unified timestamp to all requests in the transaction.
	for _, r := range reqs {
		if r != nil {
			r.Ts = startTS
		}
	}

	i.fillForwardedTxnCommitTS(reqs, startTS)
}

func forwardedTxnStartTS(reqs []*pb.Request) uint64 {
	for _, r := range reqs {
		if r != nil && r.Ts != 0 {
			return r.Ts
		}
	}
	return 0
}

func forwardedTxnMetaMutation(r *pb.Request, metaPrefix []byte) (*pb.Mutation, bool) {
	if r == nil {
		return nil, false
	}
	if r.Phase != pb.Phase_COMMIT && r.Phase != pb.Phase_ABORT {
		return nil, false
	}
	if len(r.Mutations) == 0 || r.Mutations[0] == nil {
		return nil, false
	}
	if !bytes.HasPrefix(r.Mutations[0].Key, metaPrefix) {
		return nil, false
	}
	return r.Mutations[0], true
}

func (i *Internal) fillForwardedTxnCommitTS(reqs []*pb.Request, startTS uint64) {
	const metaPrefix = "!txn|meta|"

	metaMutations := make([]*pb.Mutation, 0, len(reqs))
	prefix := []byte(metaPrefix)
	for _, r := range reqs {
		m, ok := forwardedTxnMetaMutation(r, prefix)
		if !ok {
			continue
		}
		meta, err := kv.DecodeTxnMeta(m.Value)
		if err != nil {
			continue
		}
		if meta.CommitTS != 0 {
			continue
		}
		metaMutations = append(metaMutations, m)
	}
	if len(metaMutations) == 0 {
		return
	}

	commitTS := startTS + 1
	if i.clock != nil {
		i.clock.Observe(startTS)
		commitTS = i.clock.Next()
	}

	for _, m := range metaMutations {
		meta, err := kv.DecodeTxnMeta(m.Value)
		if err != nil {
			continue
		}
		meta.CommitTS = commitTS
		m.Value = kv.EncodeTxnMeta(meta)
	}
}
