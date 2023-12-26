package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewCoordinator(txm Transactional, r *raft.Raft) *Coordinate {
	return &Coordinate{
		transactionManager: txm,
		raft:               r,
	}
}

type CoordinateResponse struct {
	CommitIndex uint64
}

type Coordinate struct {
	transactionManager Transactional
	raft               *raft.Raft
}

var _ Coordinator = (*Coordinate)(nil)

type Coordinator interface {
	Dispatch(reqs *OperationGroup[OP]) (*CoordinateResponse, error)
	IsLeader() bool
}

func (c *Coordinate) Dispatch(reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if !c.IsLeader() {
		return c.redirect(reqs)
	}

	switch reqs.IsTxn {
	case true:
		return c.dispatchTxn(reqs.Elems)
	case false:
		return c.dispatchRaw(reqs.Elems)
	}

	panic("unreachable")
}

func (c *Coordinate) IsLeader() bool {
	return c.raft.State() == raft.Leader
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

var ErrInvalidRequest = errors.New("invalid request")

func (c *Coordinate) redirect(reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	ctx := context.Background()

	if len(reqs.Elems) == 0 {
		return nil, ErrInvalidRequest
	}

	addr, _ := c.raft.LeaderWithID()

	conn, err := grpc.Dial(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

	cli := pb.NewInternalClient(conn)

	var requests []*pb.Request
	for _, req := range reqs.Elems {
		requests = append(requests, c.toRawRequest(req))
	}

	r, err := cli.Forward(ctx, c.toForwardRequest(requests))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) toForwardRequest(reqs []*pb.Request) *pb.ForwardRequest {
	if len(reqs) == 0 {
		return nil
	}

	out := &pb.ForwardRequest{
		IsTxn:    reqs[0].IsTxn,
		Requests: make([]*pb.Request, 0, len(reqs)),
	}
	out.Requests = reqs

	return out
}
