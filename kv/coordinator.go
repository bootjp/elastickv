package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

func NewCoordinator(txm Transactional) *Coordinate {
	return &Coordinate{
		transactionManager: txm,
	}
}

type CoordinateResponse struct {
	CommitIndex uint64
}

type Coordinate struct {
	transactionManager Transactional
}

var _ Coordinator = (*Coordinate)(nil)

type Coordinator interface {
	Dispatch(reqs *OperationGroup[OP]) (*CoordinateResponse, error)
}

func (c *Coordinate) Dispatch(reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	switch reqs.IsTxn {
	case true:
		return c.dispatchTxn(reqs.Elems)
	case false:
		return c.dispatchRaw(reqs.Elems)
	}

	panic("unreachable")
}

func (c *Coordinate) dispatchTxn(reqs []*Elem[OP]) (*CoordinateResponse, error) {
	var logs []*pb.Request
	for _, req := range reqs {
		m := c.toTxnRequests(req)
		logs = append(logs, m...)
	}

	r, err := c.transactionManager.Commit(logs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) dispatchRaw(req []*Elem[OP]) (*CoordinateResponse, error) {
	var logs []*pb.Request
	for _, req := range req {
		m := c.toRawRequest(req)
		logs = append(logs, m)
	}

	r, err := c.transactionManager.Commit(logs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) toRawRequest(req *Elem[OP]) *pb.Request {
	switch req.Op {
	case Put:
		return &pb.Request{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   req.Key,
					Value: req.Value,
				},
			},
		}

	case Del:
		return &pb.Request{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{
					Op:  pb.Op_DEL,
					Key: req.Key,
				},
			},
		}
	}

	panic("unreachable")
}

func (c *Coordinate) toTxnRequests(req *Elem[OP]) []*pb.Request {
	switch req.Op {
	case Put:
		return []*pb.Request{
			{
				IsTxn: true,
				Phase: pb.Phase_PREPARE,
				Mutations: []*pb.Mutation{
					{
						Key:   req.Key,
						Value: req.Value,
					},
				},
			},
			{
				IsTxn: true,
				Phase: pb.Phase_COMMIT,
				Mutations: []*pb.Mutation{
					{
						Key:   req.Key,
						Value: req.Value,
					},
				},
			},
		}

	case Del:
		return []*pb.Request{
			{
				IsTxn: true,
				Phase: pb.Phase_PREPARE,
				Mutations: []*pb.Mutation{
					{
						Key: req.Key,
					},
				},
			},
			{
				IsTxn: true,
				Phase: pb.Phase_COMMIT,
				Mutations: []*pb.Mutation{
					{
						Key: req.Key,
					},
				},
			},
		}
	}

	panic("unreachable")
}
