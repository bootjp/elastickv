package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

func NewCoordinator(txm Transactional, r *raft.Raft) *Coordinate {
	return &Coordinate{
		transactionManager: txm,
		raft:               r,
		clock:              NewHLC(),
	}
}

type CoordinateResponse struct {
	CommitIndex uint64
}

type Coordinate struct {
	transactionManager Transactional
	raft               *raft.Raft
	clock              *HLC
	connCache          GRPCConnCache
}

var _ Coordinator = (*Coordinate)(nil)

type Coordinator interface {
	Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error)
	IsLeader() bool
	VerifyLeader() error
	RaftLeader() raft.ServerAddress
	IsLeaderForKey(key []byte) bool
	VerifyLeaderForKey(key []byte) error
	RaftLeaderForKey(key []byte) raft.ServerAddress
	Clock() *HLC
}

func (c *Coordinate) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate the request before any use to avoid panics on malformed input.
	if err := validateOperationGroup(reqs); err != nil {
		return nil, err
	}

	if !c.IsLeader() {
		return c.redirect(ctx, reqs)
	}

	if reqs.IsTxn && reqs.StartTS == 0 {
		// Leader-only timestamp issuance to avoid divergence across shards.
		reqs.StartTS = c.nextStartTS()
	}

	if reqs.IsTxn {
		return c.dispatchTxn(reqs.Elems, reqs.StartTS)
	}

	return c.dispatchRaw(reqs.Elems)
}

func (c *Coordinate) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

func (c *Coordinate) VerifyLeader() error {
	return errors.WithStack(c.raft.VerifyLeader().Error())
}

// RaftLeader returns the current leader's address as known by this node.
func (c *Coordinate) RaftLeader() raft.ServerAddress {
	addr, _ := c.raft.LeaderWithID()
	return addr
}

func (c *Coordinate) Clock() *HLC {
	return c.clock
}

func (c *Coordinate) IsLeaderForKey(_ []byte) bool {
	return c.IsLeader()
}

func (c *Coordinate) VerifyLeaderForKey(_ []byte) error {
	return c.VerifyLeader()
}

func (c *Coordinate) RaftLeaderForKey(_ []byte) raft.ServerAddress {
	return c.RaftLeader()
}

func (c *Coordinate) nextStartTS() uint64 {
	return c.clock.Next()
}

func (c *Coordinate) dispatchTxn(reqs []*Elem[OP], startTS uint64) (*CoordinateResponse, error) {
	primary := primaryKeyForElems(reqs)
	if len(primary) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}

	commitTS := c.clock.Next()
	if commitTS <= startTS {
		c.clock.Observe(startTS)
		commitTS = c.clock.Next()
	}
	if commitTS <= startTS {
		return nil, errors.WithStack(ErrTxnCommitTSRequired)
	}

	logs := txnRequests(startTS, commitTS, defaultTxnLockTTLms, primary, reqs)

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
			Ts:    c.clock.Next(),
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
			Ts:    c.clock.Next(),
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

var ErrInvalidRequest = errors.New("invalid request")
var ErrLeaderNotFound = errors.New("leader not found")

func (c *Coordinate) redirect(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if len(reqs.Elems) == 0 {
		return nil, ErrInvalidRequest
	}

	addr, _ := c.raft.LeaderWithID()
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := c.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewInternalClient(conn)

	var requests []*pb.Request
	if reqs.IsTxn {
		primary := primaryKeyForElems(reqs.Elems)
		if len(primary) == 0 {
			return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
		}
		requests = txnRequests(reqs.StartTS, 0, defaultTxnLockTTLms, primary, reqs.Elems)
	} else {
		for _, req := range reqs.Elems {
			requests = append(requests, c.toRawRequest(req))
		}
	}

	r, err := cli.Forward(ctx, c.toForwardRequest(requests))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !r.Success {
		return nil, ErrInvalidRequest
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

func elemToMutation(req *Elem[OP]) *pb.Mutation {
	switch req.Op {
	case Put:
		return &pb.Mutation{
			Op:    pb.Op_PUT,
			Key:   req.Key,
			Value: req.Value,
		}
	case Del:
		return &pb.Mutation{
			Op:  pb.Op_DEL,
			Key: req.Key,
		}
	}
	panic("unreachable")
}

func txnRequests(startTS, commitTS, lockTTLms uint64, primaryKey []byte, reqs []*Elem[OP]) []*pb.Request {
	meta := &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: lockTTLms, CommitTS: 0}),
	}

	prepareMuts := make([]*pb.Mutation, 0, len(reqs)+1)
	prepareMuts = append(prepareMuts, meta)
	for _, req := range reqs {
		prepareMuts = append(prepareMuts, elemToMutation(req))
	}

	commitMeta := &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: 0, CommitTS: commitTS}),
	}
	commitMuts := make([]*pb.Mutation, 0, len(reqs)+1)
	commitMuts = append(commitMuts, commitMeta)
	for _, req := range reqs {
		commitMuts = append(commitMuts, &pb.Mutation{Op: pb.Op_PUT, Key: req.Key})
	}

	return []*pb.Request{
		{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: startTS, Mutations: prepareMuts},
		{IsTxn: true, Phase: pb.Phase_COMMIT, Ts: startTS, Mutations: commitMuts},
	}
}

func primaryKeyForElems(reqs []*Elem[OP]) []byte {
	var primary []byte
	seen := make(map[string]struct{}, len(reqs))
	for _, e := range reqs {
		if e == nil || len(e.Key) == 0 {
			continue
		}
		k := string(e.Key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		if primary == nil || bytes.Compare(e.Key, primary) < 0 {
			primary = e.Key
		}
	}
	return primary
}
