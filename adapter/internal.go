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
var ErrTxnTimestampOverflow = errors.New("txn timestamp overflow")

func (i *Internal) Forward(_ context.Context, req *pb.ForwardRequest) (*pb.ForwardResponse, error) {
	if i.raft.State() != raft.Leader {
		return nil, errors.WithStack(ErrNotLeader)
	}

	if err := i.stampTimestamps(req); err != nil {
		return &pb.ForwardResponse{
			Success:     false,
			CommitIndex: 0,
		}, errors.WithStack(err)
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

func (i *Internal) stampTimestamps(req *pb.ForwardRequest) error {
	if req == nil {
		return nil
	}
	if req.IsTxn {
		return i.stampTxnTimestamps(req.Requests)
	}

	i.stampRawTimestamps(req.Requests)
	return nil
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

func (i *Internal) stampTxnTimestamps(reqs []*pb.Request) error {
	startTS := forwardedTxnStartTS(reqs)
	if startTS == 0 {
		if i.clock == nil {
			startTS = 1
		} else {
			startTS = i.clock.Next()
		}
	}
	if startTS == ^uint64(0) {
		return errors.WithStack(ErrTxnTimestampOverflow)
	}

	// Assign the unified timestamp to all requests in the transaction.
	for _, r := range reqs {
		if r != nil {
			r.Ts = startTS
		}
	}

	return i.fillForwardedTxnCommitTS(reqs, startTS)
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

func (i *Internal) fillForwardedTxnCommitTS(reqs []*pb.Request, startTS uint64) error {
	type metaToUpdate struct {
		m    *pb.Mutation
		meta kv.TxnMeta
	}

	metaMutations := make([]metaToUpdate, 0, len(reqs))
	prefix := []byte(kv.TxnMetaPrefix)
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
		metaMutations = append(metaMutations, metaToUpdate{m: m, meta: meta})
	}
	if len(metaMutations) == 0 {
		return nil
	}

	commitTS := startTS + 1
	if commitTS == 0 {
		// Overflow: can't choose a commit timestamp strictly greater than startTS.
		return errors.WithStack(ErrTxnTimestampOverflow)
	}
	if i.clock != nil {
		i.clock.Observe(startTS)
		commitTS = i.clock.Next()
	}
	if commitTS <= startTS {
		// Defensive: avoid writing an invalid CommitTS.
		return errors.WithStack(ErrTxnTimestampOverflow)
	}

	for _, item := range metaMutations {
		item.meta.CommitTS = commitTS
		item.m.Value = kv.EncodeTxnMeta(item.meta)
	}
	return nil
}
