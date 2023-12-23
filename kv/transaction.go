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
	var commitIndex uint64

	err := func() error {
		for _, req := range reqs {
			b, err := proto.Marshal(req)
			if err != nil {
				return errors.WithStack(err)
			}

			err = t.raft.Apply(b, time.Second).Error()
			if err != nil {
				return errors.WithStack(err)
			}
			t.raft.Barrier(time.Second)
			commitIndex = t.raft.LastIndex()
		}

		return nil
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

		err = t.raft.Apply(b, time.Second).Error()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		t.raft.Barrier(time.Second)
		commitIndex = t.raft.LastIndex()
	}

	return &TransactionResponse{
		CommitIndex: commitIndex,
	}, nil
}
