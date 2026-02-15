package kv

import (
	"bytes"
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

	if reqs.IsTxn {
		return c.dispatchTxn(reqs.StartTS, reqs.Elems)
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

func (c *ShardedCoordinator) dispatchTxn(startTS uint64, elems []*Elem[OP]) (*CoordinateResponse, error) {
	grouped, gids, err := c.groupMutations(elems)
	if err != nil {
		return nil, err
	}
	primaryKey := primaryKeyForElems(elems)
	if len(primaryKey) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}

	prepared, err := c.prewriteTxn(startTS, primaryKey, grouped, gids)
	if err != nil {
		return nil, err
	}

	commitTS := c.nextTxnTSAfter(startTS)
	primaryGid, maxIndex, err := c.commitPrimaryTxn(startTS, primaryKey, grouped, commitTS)
	if err != nil {
		c.abortPreparedTxn(startTS, primaryKey, prepared, abortTSFrom(commitTS))
		return nil, errors.WithStack(err)
	}

	maxIndex = c.commitSecondaryTxns(startTS, primaryGid, primaryKey, grouped, gids, commitTS, maxIndex)
	return &CoordinateResponse{CommitIndex: maxIndex}, nil
}

type preparedGroup struct {
	gid  uint64
	keys []*pb.Mutation
}

func (c *ShardedCoordinator) prewriteTxn(startTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64) ([]preparedGroup, error) {
	prepareMeta := txnMetaMutation(primaryKey, defaultTxnLockTTLms, 0)
	prepared := make([]preparedGroup, 0, len(gids))

	for _, gid := range gids {
		g, err := c.txnGroupForID(gid)
		if err != nil {
			return nil, err
		}
		req := &pb.Request{
			IsTxn:     true,
			Phase:     pb.Phase_PREPARE,
			Ts:        startTS,
			Mutations: append([]*pb.Mutation{prepareMeta}, grouped[gid]...),
		}
		if _, err := g.Txn.Commit([]*pb.Request{req}); err != nil {
			c.abortPreparedTxn(startTS, primaryKey, prepared, c.nextTxnTSAfter(startTS))
			return nil, errors.WithStack(err)
		}
		prepared = append(prepared, preparedGroup{gid: gid, keys: keyMutations(grouped[gid])})
	}

	return prepared, nil
}

func (c *ShardedCoordinator) commitPrimaryTxn(startTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, commitTS uint64) (uint64, uint64, error) {
	primaryGid := c.engineGroupIDForKey(primaryKey)
	if primaryGid == 0 {
		return 0, 0, errors.WithStack(ErrInvalidRequest)
	}

	g, err := c.txnGroupForID(primaryGid)
	if err != nil {
		return 0, 0, err
	}

	meta := txnMetaMutation(primaryKey, 0, commitTS)
	keys := keyMutations(grouped[primaryGid])
	req := &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_COMMIT,
		Ts:        startTS,
		Mutations: append([]*pb.Mutation{meta}, keys...),
	}

	r, err := g.Txn.Commit([]*pb.Request{req})
	if err != nil {
		return primaryGid, 0, errors.WithStack(err)
	}
	if r == nil {
		return primaryGid, 0, nil
	}
	return primaryGid, r.CommitIndex, nil
}

func (c *ShardedCoordinator) commitSecondaryTxns(startTS uint64, primaryGid uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64, commitTS uint64, maxIndex uint64) uint64 {
	meta := txnMetaMutation(primaryKey, 0, commitTS)
	for _, gid := range gids {
		if gid == primaryGid {
			continue
		}
		g, ok := c.groups[gid]
		if !ok || g == nil || g.Txn == nil {
			continue
		}
		req := &pb.Request{
			IsTxn:     true,
			Phase:     pb.Phase_COMMIT,
			Ts:        startTS,
			Mutations: append([]*pb.Mutation{meta}, keyMutations(grouped[gid])...),
		}
		if r, err := g.Txn.Commit([]*pb.Request{req}); err == nil && r != nil && r.CommitIndex > maxIndex {
			maxIndex = r.CommitIndex
		}
	}
	return maxIndex
}

func (c *ShardedCoordinator) abortPreparedTxn(startTS uint64, primaryKey []byte, prepared []preparedGroup, abortTS uint64) {
	if len(prepared) == 0 {
		return
	}

	meta := txnMetaMutation(primaryKey, 0, abortTS)
	for _, pg := range prepared {
		g, ok := c.groups[pg.gid]
		if !ok || g == nil || g.Txn == nil {
			continue
		}
		req := &pb.Request{
			IsTxn:     true,
			Phase:     pb.Phase_ABORT,
			Ts:        startTS,
			Mutations: append([]*pb.Mutation{meta}, pg.keys...),
		}
		_, _ = g.Txn.Commit([]*pb.Request{req})
	}
}

func (c *ShardedCoordinator) txnGroupForID(gid uint64) (*ShardGroup, error) {
	g, ok := c.groups[gid]
	if !ok || g == nil || g.Txn == nil {
		return nil, errors.Wrapf(ErrInvalidRequest, "unknown group %d", gid)
	}
	return g, nil
}

func (c *ShardedCoordinator) nextTxnTSAfter(startTS uint64) uint64 {
	if c.clock == nil {
		return startTS + 1
	}
	ts := c.clock.Next()
	if ts <= startTS {
		c.clock.Observe(startTS)
		ts = c.clock.Next()
	}
	return ts
}

func abortTSFrom(commitTS uint64) uint64 {
	abortTS := commitTS + 1
	if abortTS == 0 {
		return commitTS
	}
	return abortTS
}

func txnMetaMutation(primaryKey []byte, lockTTLms uint64, commitTS uint64) *pb.Mutation {
	return &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: lockTTLms, CommitTS: commitTS}),
	}
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
		return maxTS + 1, nil
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

func (c *ShardedCoordinator) engineGroupIDForKey(key []byte) uint64 {
	route, ok := c.engine.GetRoute(routeKey(key))
	if !ok {
		return 0
	}
	return route.GroupID
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
	// NOTE: ShardedCoordinator implements distributed transactions directly in
	// Dispatch. txnLogs is retained for compatibility and single-shard helpers.
	grouped, gids, err := c.groupMutations(reqs.Elems)
	if err != nil {
		return nil, err
	}
	if len(gids) != 1 {
		return nil, errors.WithStack(ErrInvalidRequest)
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
			&pb.Request{
				IsTxn: true,
				Phase: pb.Phase_PREPARE,
				Ts:    startTS,
				Mutations: append([]*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKeyFromMutations(muts), LockTTLms: defaultTxnLockTTLms})},
				}, muts...),
			},
			&pb.Request{
				IsTxn: true,
				Phase: pb.Phase_COMMIT,
				Ts:    startTS,
				Mutations: append([]*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKeyFromMutations(muts)})},
				}, keyMutations(muts)...),
			},
		)
	}
	return logs
}

func primaryKeyFromMutations(muts []*pb.Mutation) []byte {
	if len(muts) == 0 {
		return nil
	}
	primary := muts[0].Key
	for _, m := range muts[1:] {
		if m == nil || len(m.Key) == 0 {
			continue
		}
		if bytes.Compare(m.Key, primary) < 0 {
			primary = m.Key
		}
	}
	return primary
}

func keyMutations(muts []*pb.Mutation) []*pb.Mutation {
	out := make([]*pb.Mutation, 0, len(muts))
	seen := map[string]struct{}{}
	for _, m := range muts {
		if m == nil || len(m.Key) == 0 {
			continue
		}
		k := string(m.Key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, &pb.Mutation{Op: pb.Op_PUT, Key: m.Key})
	}
	return out
}
