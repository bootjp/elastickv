package kv

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

type ShardGroup struct {
	Engine raftengine.Engine
	Store  store.MVCCStore
	Txn    Transactional
}

const (
	txnPhaseCount = 2

	txnSecondaryCommitRetryAttempts = 3
	txnSecondaryCommitRetryBackoff  = 20 * time.Millisecond
)

// ShardedCoordinator routes operations to shard-specific raft groups.
// It issues timestamps via a shared HLC and uses ShardRouter to dispatch.
type ShardedCoordinator struct {
	engine       *distribution.Engine
	router       *ShardRouter
	groups       map[uint64]*ShardGroup
	defaultGroup uint64
	clock        *HLC
	store        store.MVCCStore
	log          *slog.Logger
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
		log:          slog.Default(),
	}
}

func (c *ShardedCoordinator) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := validateOperationGroup(reqs); err != nil {
		return nil, err
	}

	// DEL_PREFIX cannot be routed to a single shard because the prefix may
	// span multiple shards (or be nil, meaning "all keys"). Broadcast the
	// operation to every shard group so each FSM scans locally.
	if hasDelPrefixElem(reqs.Elems) {
		return c.dispatchDelPrefixBroadcast(reqs.IsTxn, reqs.Elems)
	}

	if reqs.IsTxn && reqs.StartTS == 0 {
		startTS, err := c.nextStartTS(ctx, reqs.Elems)
		if err != nil {
			return nil, err
		}
		reqs.StartTS = startTS
		// When the coordinator assigns StartTS, also clear any caller-provided
		// CommitTS so dispatchTxn generates both timestamps consistently.
		// A caller-supplied CommitTS without a matching StartTS could produce
		// CommitTS <= StartTS (an invalid transaction).
		reqs.CommitTS = 0
	}

	if reqs.IsTxn {
		return c.dispatchTxn(ctx, reqs.StartTS, reqs.CommitTS, reqs.Elems, reqs.ReadKeys)
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

// hasDelPrefixElem returns true if any element is a DelPrefix operation.
func hasDelPrefixElem(elems []*Elem[OP]) bool {
	for _, e := range elems {
		if e != nil && e.Op == DelPrefix {
			return true
		}
	}
	return false
}

// validateDelPrefixOnly ensures all elements are DelPrefix operations.
// Mixing DEL_PREFIX with other operations (PUT, DEL) in a single dispatch is
// not allowed because the FSM handles DEL_PREFIX exclusively.
func validateDelPrefixOnly(elems []*Elem[OP]) error {
	for _, e := range elems {
		if e != nil && e.Op != DelPrefix {
			return errors.Wrap(ErrInvalidRequest, "DEL_PREFIX cannot be mixed with other operations")
		}
	}
	return nil
}

// dispatchDelPrefixBroadcast validates and broadcasts DEL_PREFIX operations
// to every shard group. Each element becomes a separate pb.Request (the FSM's
// extractDelPrefix processes only the first DEL_PREFIX mutation per request).
// All requests are batched into a single Commit call per shard group.
func (c *ShardedCoordinator) dispatchDelPrefixBroadcast(isTxn bool, elems []*Elem[OP]) (*CoordinateResponse, error) {
	if isTxn {
		return nil, errors.Wrap(ErrInvalidRequest, "DEL_PREFIX not supported in transactions")
	}
	if err := validateDelPrefixOnly(elems); err != nil {
		return nil, err
	}

	ts := c.clock.Next()
	requests := make([]*pb.Request, 0, len(elems))
	for _, elem := range elems {
		requests = append(requests, &pb.Request{
			IsTxn:     false,
			Phase:     pb.Phase_NONE,
			Ts:        ts,
			Mutations: []*pb.Mutation{elemToMutation(elem)},
		})
	}

	return c.broadcastToAllGroups(requests)
}

// broadcastToAllGroups sends the same set of requests to every shard group in
// parallel and returns the maximum commit index.
func (c *ShardedCoordinator) broadcastToAllGroups(requests []*pb.Request) (*CoordinateResponse, error) {
	var (
		maxIndex atomic.Uint64
		firstErr error
		errMu    sync.Mutex
		wg       sync.WaitGroup
	)
	for _, g := range c.groups {
		wg.Add(1)
		go func(g *ShardGroup) {
			defer wg.Done()
			r, err := g.Txn.Commit(requests)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			if r != nil {
				for {
					cur := maxIndex.Load()
					if r.CommitIndex <= cur || maxIndex.CompareAndSwap(cur, r.CommitIndex) {
						break
					}
				}
			}
		}(g)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, errors.WithStack(firstErr)
	}
	return &CoordinateResponse{CommitIndex: maxIndex.Load()}, nil
}

func (c *ShardedCoordinator) dispatchTxn(ctx context.Context, startTS uint64, commitTS uint64, elems []*Elem[OP], readKeys [][]byte) (*CoordinateResponse, error) {
	grouped, gids, err := c.groupMutations(elems)
	if err != nil {
		return nil, err
	}
	primaryKey := primaryKeyForElems(elems)
	if len(primaryKey) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}

	commitTS, err = c.resolveTxnCommitTS(startTS, commitTS)
	if err != nil {
		return nil, err
	}

	if len(gids) == 1 {
		return c.dispatchSingleShardTxn(startTS, commitTS, primaryKey, gids[0], elems, readKeys)
	}

	prepared, err := c.prewriteTxn(ctx, startTS, commitTS, primaryKey, grouped, gids, readKeys)
	if err != nil {
		return nil, err
	}

	primaryGid, maxIndex, err := c.commitPrimaryTxn(startTS, primaryKey, grouped, commitTS)
	if err != nil {
		c.abortPreparedTxn(startTS, primaryKey, prepared, abortTSFrom(startTS, commitTS))
		return nil, errors.WithStack(err)
	}

	maxIndex = c.commitSecondaryTxns(startTS, primaryGid, primaryKey, grouped, gids, commitTS, maxIndex)
	return &CoordinateResponse{CommitIndex: maxIndex}, nil
}

func (c *ShardedCoordinator) resolveTxnCommitTS(startTS, commitTS uint64) (uint64, error) {
	if commitTS == 0 {
		commitTS = c.nextTxnTSAfter(startTS)
	} else {
		// Observe caller-provided commitTS to keep the HLC monotonic; without
		// this the clock could later issue timestamps smaller than commitTS.
		c.clock.Observe(commitTS)
	}
	if commitTS == 0 || commitTS <= startTS {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return commitTS, nil
}

func (c *ShardedCoordinator) dispatchSingleShardTxn(startTS, commitTS uint64, primaryKey []byte, gid uint64, elems []*Elem[OP], readKeys [][]byte) (*CoordinateResponse, error) {
	g, err := c.txnGroupForID(gid)
	if err != nil {
		return nil, err
	}
	// Single-shard: read-set validated pre-Raft by the adapter.
	resp, err := g.Txn.Commit([]*pb.Request{
		onePhaseTxnRequest(startTS, commitTS, primaryKey, elems, nil),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp == nil {
		return &CoordinateResponse{}, nil
	}
	return &CoordinateResponse{CommitIndex: resp.CommitIndex}, nil
}

type preparedGroup struct {
	gid  uint64
	keys []*pb.Mutation
}

func (c *ShardedCoordinator) prewriteTxn(ctx context.Context, startTS, commitTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64, readKeys [][]byte) ([]preparedGroup, error) {
	prepareMeta := txnMetaMutation(primaryKey, defaultTxnLockTTLms, 0)
	prepared := make([]preparedGroup, 0, len(gids))

	groupedReadKeys := c.groupReadKeysByShardID(readKeys)

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
			ReadKeys:  groupedReadKeys[gid],
		}
		if _, err := g.Txn.Commit([]*pb.Request{req}); err != nil {
			c.abortPreparedTxn(startTS, primaryKey, prepared, abortTSFrom(startTS, commitTS))
			return nil, errors.WithStack(err)
		}
		prepared = append(prepared, preparedGroup{gid: gid, keys: keyMutations(grouped[gid])})
	}

	// Validate read keys on read-only shards (shards that have read keys
	// but no mutations in this transaction). Without this, a concurrent
	// write to a read-only shard would go undetected.
	if err := c.validateReadOnlyShards(ctx, groupedReadKeys, gids, startTS); err != nil {
		c.abortPreparedTxn(startTS, primaryKey, prepared, abortTSFrom(startTS, commitTS))
		return nil, err
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
	// Secondary commits are best-effort. If a shard is unavailable after the
	// primary commits, read-time lock resolution will commit the remaining
	// secondaries based on the primary commit record. Retry a few times to
	// absorb short leader-election windows and reduce lock lag.
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
		r, err := commitSecondaryWithRetry(g, req)
		if err != nil {
			c.logger().Warn("txn secondary commit failed",
				slog.Uint64("gid", gid),
				slog.String("primary_key", string(primaryKey)),
				slog.Uint64("start_ts", startTS),
				slog.Uint64("commit_ts", commitTS),
				slog.Any("err", err),
			)
			continue
		}
		if r != nil && r.CommitIndex > maxIndex {
			maxIndex = r.CommitIndex
		}
	}
	return maxIndex
}

func commitSecondaryWithRetry(g *ShardGroup, req *pb.Request) (*TransactionResponse, error) {
	if g == nil || g.Txn == nil || req == nil {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	var lastErr error
	for attempt := range txnSecondaryCommitRetryAttempts {
		resp, err := g.Txn.Commit([]*pb.Request{req})
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt+1 < txnSecondaryCommitRetryAttempts {
			time.Sleep(txnSecondaryCommitRetryBackoff * time.Duration(attempt+1))
		}
	}
	return nil, errors.WithStack(lastErr)
}

func (c *ShardedCoordinator) logger() *slog.Logger {
	if c != nil && c.log != nil {
		return c.log
	}
	return slog.Default()
}

func (c *ShardedCoordinator) abortPreparedTxn(startTS uint64, primaryKey []byte, prepared []preparedGroup, abortTS uint64) {
	if len(prepared) == 0 {
		return
	}
	if abortTS == 0 || abortTS <= startTS {
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
		if _, err := g.Txn.Commit([]*pb.Request{req}); err != nil {
			if errors.Is(err, ErrTxnAlreadyCommitted) {
				continue
			}
			c.logger().Warn("txn abort failed; locks may remain until TTL expiry",
				slog.Uint64("gid", pg.gid),
				slog.String("primary_key", string(primaryKey)),
				slog.Uint64("start_ts", startTS),
				slog.Uint64("abort_ts", abortTS),
				slog.Any("err", err),
			)
		}
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
		nextTS := startTS + 1
		if nextTS == 0 {
			return 0
		}
		return nextTS
	}
	ts := c.clock.Next()
	if ts <= startTS {
		c.clock.Observe(startTS)
		ts = c.clock.Next()
	}
	if ts <= startTS {
		return 0
	}
	return ts
}

func abortTSFrom(startTS, commitTS uint64) uint64 {
	const maxUint64 = ^uint64(0)

	// Prefer commitTS+1 when representable and strictly greater than startTS.
	if commitTS < maxUint64 {
		abortTS := commitTS + 1
		if abortTS > startTS {
			return abortTS
		}
	}

	// Fallback to startTS+1 when representable.
	if startTS < maxUint64 {
		return startTS + 1
	}

	// No representable timestamp exists that is strictly greater than startTS.
	return 0
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
	if !ok {
		return false
	}
	return isLeaderEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) VerifyLeader() error {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return verifyLeaderEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) RaftLeader() raft.ServerAddress {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return ""
	}
	return leaderAddrFromEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return linearizableReadEngineCtx(ctx, engineForGroup(g))
}

func (c *ShardedCoordinator) IsLeaderForKey(key []byte) bool {
	g, ok := c.groupForKey(key)
	if !ok {
		return false
	}
	return isLeaderEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) VerifyLeaderForKey(key []byte) error {
	g, ok := c.groupForKey(key)
	if !ok {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return verifyLeaderEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) RaftLeaderForKey(key []byte) raft.ServerAddress {
	g, ok := c.groupForKey(key)
	if !ok {
		return ""
	}
	return leaderAddrFromEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error) {
	g, ok := c.groupForKey(key)
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return linearizableReadEngineCtx(ctx, engineForGroup(g))
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

func (c *ShardedCoordinator) groupReadKeysByShardID(readKeys [][]byte) map[uint64][][]byte {
	if len(readKeys) == 0 {
		return nil
	}
	grouped := make(map[uint64][][]byte)
	for _, key := range readKeys {
		gid := c.engineGroupIDForKey(key)
		if gid == 0 {
			continue
		}
		grouped[gid] = append(grouped[gid], key)
	}
	return grouped
}

// validateReadOnlyShards checks read-write conflicts on shards that have
// read keys but no mutations in this transaction. writeGIDs is the set of
// shards that already received a PREPARE with their readKeys attached.
//
// Because these shards have no mutations, we cannot send a PREPARE request
// (the FSM rejects empty mutation lists). Instead we issue a linearizable
// read barrier on each read-only shard's Raft group (ensuring the local
// FSM has applied all committed log entries) and then check LatestCommitTS
// against the local store.
//
// NOTE: This check is performed outside the FSM's applyMu lock, so there
// is a small TOCTOU window between the linearizable read barrier and the
// LatestCommitTS check. A concurrent write that commits in this window may
// go undetected. Full SSI for read-only shards in multi-shard transactions
// would require a dedicated "read-validate" FSM request phase. For
// single-shard transactions and write-shard read keys, validation is fully
// atomic under applyMu.
func (c *ShardedCoordinator) validateReadOnlyShards(ctx context.Context, groupedReadKeys map[uint64][][]byte, writeGIDs []uint64, startTS uint64) error {
	if len(groupedReadKeys) == 0 {
		return nil
	}
	writeSet := make(map[uint64]struct{}, len(writeGIDs))
	for _, gid := range writeGIDs {
		writeSet[gid] = struct{}{}
	}
	for gid, keys := range groupedReadKeys {
		if _, isWrite := writeSet[gid]; isWrite {
			continue
		}
		g, ok := c.groups[gid]
		if !ok {
			continue
		}
		// Linearizable read barrier: wait until the shard's FSM has applied
		// all Raft-committed entries so LatestCommitTS reflects the latest
		// committed state. Without this, a concurrent write that is committed
		// in Raft but not yet applied locally would be invisible.
		if _, err := linearizableReadEngineCtx(ctx, engineForGroup(g)); err != nil {
			return errors.WithStack(err)
		}
		for _, key := range keys {
			ts, exists, err := g.Store.LatestCommitTS(ctx, key)
			if err != nil {
				return errors.WithStack(err)
			}
			if exists && ts > startTS {
				return errors.WithStack(store.NewWriteConflictError(key))
			}
		}
	}
	return nil
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
	return c.rawLogs(reqs)
}

func (c *ShardedCoordinator) rawLogs(reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	grouped, gids, err := c.groupMutations(reqs.Elems)
	if err != nil {
		return nil, err
	}

	logs := make([]*pb.Request, 0, len(gids))
	for _, gid := range gids {
		logs = append(logs, &pb.Request{
			IsTxn:     false,
			Phase:     pb.Phase_NONE,
			Ts:        c.clock.Next(),
			Mutations: grouped[gid],
		})
	}
	return logs, nil
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
	commitTS, err := c.resolveTxnCommitTS(reqs.StartTS, reqs.CommitTS)
	if err != nil {
		return nil, err
	}
	return buildTxnLogs(reqs.StartTS, commitTS, grouped, gids)
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
	slices.Sort(gids)
	return grouped, gids, nil
}

func buildTxnLogs(startTS uint64, commitTS uint64, grouped map[uint64][]*pb.Mutation, gids []uint64) ([]*pb.Request, error) {
	logs := make([]*pb.Request, 0, len(gids)*txnPhaseCount)
	for _, gid := range gids {
		muts := grouped[gid]
		primaryKey, keys := primaryKeyAndKeyMutations(muts)
		if len(primaryKey) == 0 {
			return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
		}
		logs = append(logs,
			&pb.Request{
				IsTxn: true,
				Phase: pb.Phase_PREPARE,
				Ts:    startTS,
				Mutations: append([]*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
				}, muts...),
			},
			&pb.Request{
				IsTxn: true,
				Phase: pb.Phase_COMMIT,
				Ts:    startTS,
				Mutations: append([]*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: 0, CommitTS: commitTS})},
				}, keys...),
			},
		)
	}
	return logs, nil
}

func primaryKeyAndKeyMutations(muts []*pb.Mutation) ([]byte, []*pb.Mutation) {
	seen := map[string]struct{}{}
	var primary []byte
	keys := make([]*pb.Mutation, 0, len(muts))
	for _, m := range muts {
		if m == nil || len(m.Key) == 0 {
			continue
		}
		k := string(m.Key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		if primary == nil || bytes.Compare(m.Key, primary) < 0 {
			primary = m.Key
		}
		keys = append(keys, &pb.Mutation{Op: pb.Op_PUT, Key: m.Key})
	}
	return primary, keys
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
