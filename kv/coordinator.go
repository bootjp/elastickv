package kv

import (
	"bytes"
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const redirectForwardTimeout = 5 * time.Second

func NewCoordinator(txm Transactional, r *raft.Raft) *Coordinate {
	return NewCoordinatorWithEngine(txm, hashicorpraftengine.New(r))
}

func NewCoordinatorWithEngine(txm Transactional, engine raftengine.Engine) *Coordinate {
	return &Coordinate{
		transactionManager: txm,
		engine:             engine,
		clock:              NewHLC(),
	}
}

type CoordinateResponse struct {
	CommitIndex uint64
}

type Coordinate struct {
	transactionManager Transactional
	engine             raftengine.Engine
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
		// When the leader assigns StartTS, also clear any caller-provided
		// CommitTS so dispatchTxn generates both timestamps consistently.
		// A caller-supplied CommitTS without a matching StartTS could produce
		// CommitTS <= StartTS (an invalid transaction).
		reqs.StartTS = c.nextStartTS()
		reqs.CommitTS = 0
	}

	if reqs.IsTxn {
		return c.dispatchTxn(reqs.Elems, reqs.ReadKeys, reqs.StartTS, reqs.CommitTS)
	}

	return c.dispatchRaw(reqs.Elems)
}

func (c *Coordinate) IsLeader() bool {
	return isLeaderEngine(c.engine)
}

func (c *Coordinate) VerifyLeader() error {
	return verifyLeaderEngine(c.engine)
}

// RaftLeader returns the current leader's address as known by this node.
func (c *Coordinate) RaftLeader() raft.ServerAddress {
	return leaderAddrFromEngine(c.engine)
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

func (c *Coordinate) LinearizableRead(ctx context.Context) (uint64, error) {
	return linearizableReadEngineCtx(ctx, c.engine)
}

func (c *Coordinate) LinearizableReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LinearizableRead(ctx)
}

func (c *Coordinate) nextStartTS() uint64 {
	return c.clock.Next()
}

func (c *Coordinate) dispatchTxn(reqs []*Elem[OP], readKeys [][]byte, startTS uint64, commitTS uint64) (*CoordinateResponse, error) {
	if len(readKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	primary := primaryKeyForElems(reqs)
	if len(primary) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}

	if commitTS == 0 {
		commitTS = c.clock.Next()
		if commitTS <= startTS {
			c.clock.Observe(startTS)
			commitTS = c.clock.Next()
		}
	} else {
		// Observe the caller-provided commitTS so the HLC never issues
		// a smaller timestamp in subsequent calls, preserving monotonicity.
		c.clock.Observe(commitTS)
	}
	if commitTS <= startTS {
		return nil, errors.WithStack(ErrTxnCommitTSRequired)
	}

	// ReadKeys are included in the Raft log entry so the FSM validates
	// read-write conflicts atomically under applyMu, eliminating the TOCTOU
	// window that exists between the adapter's pre-Raft validateReadSet call
	// and FSM application. The adapter's validateReadSet is kept as a fast
	// path to fail early without a Raft round-trip, but the FSM check is
	// the authoritative, serializable validation.
	r, err := c.transactionManager.Commit([]*pb.Request{
		onePhaseTxnRequest(startTS, commitTS, primary, reqs, readKeys),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) dispatchRaw(req []*Elem[OP]) (*CoordinateResponse, error) {
	muts := make([]*pb.Mutation, 0, len(req))
	for _, elem := range req {
		muts = append(muts, elemToMutation(elem))
	}

	logs := []*pb.Request{{
		IsTxn:     false,
		Phase:     pb.Phase_NONE,
		Ts:        c.clock.Next(),
		Mutations: muts,
	}}

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

	case DelPrefix:
		return &pb.Request{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    c.clock.Next(),
			Mutations: []*pb.Mutation{
				{
					Op:  pb.Op_DEL_PREFIX,
					Key: req.Key,
				},
			},
		}
	}

	panic("unreachable")
}

var ErrInvalidRequest = errors.New("invalid request")

// maxReadKeys caps the number of keys that may appear in a transaction's read
// set. Exceeding this limit is rejected to prevent unbounded memory growth.
const maxReadKeys = 10_000

var ErrLeaderNotFound = errors.New("leader not found")

func (c *Coordinate) redirect(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if len(reqs.Elems) == 0 {
		return nil, ErrInvalidRequest
	}

	addr := leaderAddrFromEngine(c.engine)
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
		if len(reqs.ReadKeys) > maxReadKeys {
			return nil, errors.WithStack(ErrInvalidRequest)
		}
		primary := primaryKeyForElems(reqs.Elems)
		if len(primary) == 0 {
			return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
		}
		// When StartTS is absent (leader will assign it), also clear CommitTS
		// so the leader assigns both timestamps consistently. A caller-provided
		// CommitTS without a StartTS would produce an invalid txn where
		// CommitTS <= StartTS (because StartTS=0 at the forwarding site).
		commitTS := reqs.CommitTS
		if reqs.StartTS == 0 {
			commitTS = 0
		}
		requests = []*pb.Request{
			onePhaseTxnRequest(reqs.StartTS, commitTS, primary, reqs.Elems, reqs.ReadKeys),
		}
	} else {
		for _, req := range reqs.Elems {
			requests = append(requests, c.toRawRequest(req))
		}
	}

	fwdCtx, cancel := context.WithTimeout(ctx, redirectForwardTimeout)
	defer cancel()
	r, err := cli.Forward(fwdCtx, c.toForwardRequest(requests))
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
	case DelPrefix:
		return &pb.Mutation{
			Op:  pb.Op_DEL_PREFIX,
			Key: req.Key, // prefix (may be empty for "all keys")
		}
	}
	panic("unreachable")
}

func onePhaseTxnRequest(startTS, commitTS uint64, primaryKey []byte, reqs []*Elem[OP], readKeys [][]byte) *pb.Request {
	muts := make([]*pb.Mutation, 0, len(reqs)+1)
	muts = append(muts, txnMetaMutation(primaryKey, 0, commitTS))
	for _, req := range reqs {
		muts = append(muts, elemToMutation(req))
	}
	return &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_NONE,
		Ts:        startTS,
		Mutations: muts,
		ReadKeys:  readKeys,
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
