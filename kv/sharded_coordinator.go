package kv

import (
	"context"
	"sort"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

type ShardGroup struct {
	Raft  *raft.Raft
	Store store.MVCCStore
	Txn   Transactional
}

const txnPhaseCount = 2

// ShardedCoordinator routes operations to shard-specific raft groups.
// It issues timestamps via a shared HLC and uses ShardRouter to dispatch.
type ShardedCoordinator struct {
	engine       *distribution.Engine
	router       *ShardRouter
	groups       map[uint64]*ShardGroup
	defaultGroup uint64
	clock        *HLC
	store        store.MVCCStore
}

// NewShardedCoordinator builds a coordinator for the provided shard groups.
// The defaultGroup is used for non-keyed leader checks.
func NewShardedCoordinator(engine *distribution.Engine, groups map[uint64]*ShardGroup, defaultGroup uint64, clock *HLC, st store.MVCCStore) *ShardedCoordinator {
	router := NewShardRouter(engine)
	for gid, g := range groups {
		router.Register(gid, g.Txn, g.Store)
	}
	return &ShardedCoordinator{
		engine:       engine,
		router:       router,
		groups:       groups,
		defaultGroup: defaultGroup,
		clock:        clock,
		store:        st,
	}
}

func (c *ShardedCoordinator) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := validateOperationGroup(reqs); err != nil {
		return nil, err
	}

	if reqs.IsTxn && reqs.StartTS == 0 {
		startTS, err := c.nextStartTS(ctx, reqs.Elems)
		if err != nil {
			return nil, err
		}
		reqs.StartTS = startTS
	}

	logs, err := c.requestLogs(reqs)
	if err != nil {
		return nil, err
	}

	r, err := c.router.Commit(logs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CoordinateResponse{CommitIndex: r.CommitIndex}, nil
}

func (c *ShardedCoordinator) nextStartTS(ctx context.Context, elems []*Elem[OP]) (uint64, error) {
	maxTS, err := c.maxLatestCommitTS(ctx, elems)
	if err != nil {
		return 0, err
	}
	if c.clock != nil && maxTS > 0 {
		c.clock.Observe(maxTS)
	}
	if c.clock == nil {
		return maxTS, nil
	}
	return c.clock.Next(), nil
}

func (c *ShardedCoordinator) maxLatestCommitTS(ctx context.Context, elems []*Elem[OP]) (uint64, error) {
	if c.store == nil {
		return 0, nil
	}

	keys := make([][]byte, 0, len(elems))
	for _, e := range elems {
		if e == nil || len(e.Key) == 0 {
			continue
		}
		keys = append(keys, e.Key)
	}

	return MaxLatestCommitTS(ctx, c.store, keys)
}

func (c *ShardedCoordinator) IsLeader() bool {
	g, ok := c.groups[c.defaultGroup]
	if !ok || g.Raft == nil {
		return false
	}
	return g.Raft.State() == raft.Leader
}

func (c *ShardedCoordinator) VerifyLeader() error {
	g, ok := c.groups[c.defaultGroup]
	if !ok || g.Raft == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return errors.WithStack(g.Raft.VerifyLeader().Error())
}

func (c *ShardedCoordinator) RaftLeader() raft.ServerAddress {
	g, ok := c.groups[c.defaultGroup]
	if !ok || g.Raft == nil {
		return ""
	}
	addr, _ := g.Raft.LeaderWithID()
	return addr
}

func (c *ShardedCoordinator) IsLeaderForKey(key []byte) bool {
	g, ok := c.groupForKey(key)
	if !ok || g.Raft == nil {
		return false
	}
	return g.Raft.State() == raft.Leader
}

func (c *ShardedCoordinator) VerifyLeaderForKey(key []byte) error {
	g, ok := c.groupForKey(key)
	if !ok || g.Raft == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return errors.WithStack(g.Raft.VerifyLeader().Error())
}

func (c *ShardedCoordinator) RaftLeaderForKey(key []byte) raft.ServerAddress {
	g, ok := c.groupForKey(key)
	if !ok || g.Raft == nil {
		return ""
	}
	addr, _ := g.Raft.LeaderWithID()
	return addr
}

func (c *ShardedCoordinator) Clock() *HLC {
	return c.clock
}

func (c *ShardedCoordinator) groupForKey(key []byte) (*ShardGroup, bool) {
	route, ok := c.engine.GetRoute(routeKey(key))
	if !ok {
		return nil, false
	}
	g, ok := c.groups[route.GroupID]
	return g, ok
}

func (c *ShardedCoordinator) toRawRequest(req *Elem[OP]) *pb.Request {
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

var _ Coordinator = (*ShardedCoordinator)(nil)

func validateOperationGroup(reqs *OperationGroup[OP]) error {
	if reqs == nil || len(reqs.Elems) == 0 {
		return ErrInvalidRequest
	}
	for _, e := range reqs.Elems {
		if e == nil {
			return ErrInvalidRequest
		}
	}
	return nil
}

func (c *ShardedCoordinator) requestLogs(reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	if reqs.IsTxn {
		return c.txnLogs(reqs)
	}
	return c.rawLogs(reqs), nil
}

func (c *ShardedCoordinator) rawLogs(reqs *OperationGroup[OP]) []*pb.Request {
	logs := make([]*pb.Request, 0, len(reqs.Elems))
	for _, req := range reqs.Elems {
		logs = append(logs, c.toRawRequest(req))
	}
	return logs
}

func (c *ShardedCoordinator) txnLogs(reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	grouped, gids, err := c.groupMutations(reqs.Elems)
	if err != nil {
		return nil, err
	}
	if len(gids) > 1 {
		return nil, errors.Wrapf(
			ErrCrossShardTransactionNotSupported,
			"involved_shards=%v",
			gids,
		)
	}
	return buildTxnLogs(reqs.StartTS, grouped, gids), nil
}

func (c *ShardedCoordinator) groupMutations(reqs []*Elem[OP]) (map[uint64][]*pb.Mutation, []uint64, error) {
	grouped := make(map[uint64][]*pb.Mutation)
	for _, req := range reqs {
		if req == nil {
			return nil, nil, ErrInvalidRequest
		}
		mut := elemToMutation(req)
		route, ok := c.engine.GetRoute(routeKey(mut.Key))
		if !ok {
			return nil, nil, errors.Wrapf(ErrInvalidRequest, "no route for key %q", mut.Key)
		}
		grouped[route.GroupID] = append(grouped[route.GroupID], mut)
	}
	gids := make([]uint64, 0, len(grouped))
	for gid := range grouped {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool { return gids[i] < gids[j] })
	return grouped, gids, nil
}

func buildTxnLogs(startTS uint64, grouped map[uint64][]*pb.Mutation, gids []uint64) []*pb.Request {
	logs := make([]*pb.Request, 0, len(gids)*txnPhaseCount)
	for _, gid := range gids {
		muts := grouped[gid]
		logs = append(logs,
			&pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: startTS, Mutations: muts},
			&pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Ts: startTS, Mutations: muts},
		)
	}
	return logs
}
