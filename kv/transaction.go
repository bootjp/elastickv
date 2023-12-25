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

func (t *TransactionManager) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	commitIndex, err := func() (uint64, error) {
		commitIndex := uint64(0)
		for _, req := range reqs {
			b, err := proto.Marshal(req)
			if err != nil {
				return 0, errors.WithStack(err)
			}

			af := t.raft.Apply(b, time.Second)
			if af.Error() != nil {
				return 0, errors.WithStack(af.Error())
			}
			f := t.raft.Barrier(time.Second)
			if f.Error() != nil {
				return 0, errors.WithStack(f.Error())
			}
			commitIndex = af.Index()
		}

		return commitIndex, nil
	}()

	if err != nil {
		_, _err := t.Abort(reqs)
		if _err != nil {
			return nil, errors.WithStack(errors.CombineErrors(err, _err))
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
		abortReqs = append(abortReqs, &pb.Request{
			IsTxn:     true,
			Phase:     pb.Phase_ABORT,
			Ts:        req.Ts,
			Mutations: req.Mutations,
		})
	}

	var commitIndex uint64
	for _, req := range abortReqs {
		b, err := proto.Marshal(req)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		af := t.raft.Apply(b, time.Second)
		if af.Error() != nil {
			return nil, errors.WithStack(af.Error())
		}
		f := t.raft.Barrier(time.Second)
		if f.Error() != nil {
			return nil, errors.WithStack(f.Error())
		}
		commitIndex = af.Index()
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}
