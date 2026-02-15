package kv

import (
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type TransactionManager struct {
	raft *raft.Raft
}

func NewTransaction(raft *raft.Raft) *TransactionManager {
	return &TransactionManager{
		raft: raft,
	}
}

type Transactional interface {
	Commit(reqs []*pb.Request) (*TransactionResponse, error)
	Abort(reqs []*pb.Request) (*TransactionResponse, error)
}

type TransactionResponse struct {
	CommitIndex uint64
}

// applyAndBarrier submits a log entry, waits for it to be applied, and
// surfaces both Raft transport errors and errors returned from FSM.Apply.
// HashiCorp Raft delivers FSM errors via ApplyFuture.Response(), not Error(),
// so we must inspect the response to avoid silently treating failed writes as
// successes.
func applyAndBarrier(r *raft.Raft, b []byte) (uint64, error) {
	af := r.Apply(b, time.Second)
	if err := af.Error(); err != nil {
		return 0, errors.WithStack(err)
	}

	if resp := af.Response(); resp != nil {
		if err, ok := resp.(error); ok && err != nil {
			return 0, errors.WithStack(err)
		}
	}

	if f := r.Barrier(time.Second); f.Error() != nil {
		return 0, errors.WithStack(f.Error())
	}

	return af.Index(), nil
}

func (t *TransactionManager) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	commitIndex, err := func() (uint64, error) {
		commitIndex := uint64(0)
		for _, req := range reqs {
			b, err := proto.Marshal(req)
			if err != nil {
				return 0, errors.WithStack(err)
			}

			idx, err := applyAndBarrier(t.raft, b)
			if err != nil {
				return 0, err
			}
			commitIndex = idx
		}

		return commitIndex, nil
	}()

	if err != nil {
		// Only attempt transactional cleanup for transactional batches. Raw request
		// batches may partially succeed across shards by design.
		if len(reqs) > 0 && reqs[0] != nil && reqs[0].IsTxn {
			_, _err := t.Abort(reqs)
			if _err != nil {
				return nil, errors.WithStack(errors.CombineErrors(err, _err))
			}
		}
		return nil, errors.WithStack(err)
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}

func (t *TransactionManager) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	var abortReqs []*pb.Request
	for _, req := range reqs {
		if req == nil || !req.IsTxn {
			continue
		}
		meta, muts, err := extractTxnMeta(req.Mutations)
		if err != nil {
			// Best-effort cleanup; skip requests we can't interpret.
			continue
		}
		startTS := req.Ts
		abortTS := startTS + 1
		meta.CommitTS = abortTS

		abortReqs = append(abortReqs, &pb.Request{
			IsTxn: true,
			Phase: pb.Phase_ABORT,
			Ts:    startTS,
			Mutations: append([]*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   []byte(txnMetaPrefix),
					Value: EncodeTxnMeta(meta),
				},
			}, muts...),
		})
	}

	var commitIndex uint64
	for _, req := range abortReqs {
		b, err := proto.Marshal(req)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		idx, err := applyAndBarrier(t.raft, b)
		if err != nil {
			return nil, err
		}
		commitIndex = idx
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}

func extractTxnMeta(muts []*pb.Mutation) (TxnMeta, []*pb.Mutation, error) {
	if len(muts) == 0 || muts[0] == nil || !isTxnMetaKey(muts[0].Key) {
		return TxnMeta{}, nil, errors.WithStack(ErrTxnMetaMissing)
	}
	meta, err := DecodeTxnMeta(muts[0].Value)
	if err != nil {
		return TxnMeta{}, nil, errors.WithStack(err)
	}
	return meta, muts[1:], nil
}
