package kv

import (
	"bytes"
	"context"
	"io"
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
)

type ShardGroup struct {
	Engine raftengine.Engine
	Store  store.MVCCStore
	Txn    Transactional
	lease  leaseState
}

// leaseRefreshingTxn wraps a Transactional so every Commit / Abort that
// produced a real Raft commit extends its shard's lease. Mirrors
// Coordinate.Dispatch's lease hook for the per-shard case.
//
// Both TransactionManager.Commit and .Abort can return success WITHOUT
// going through Raft -- Commit short-circuits on empty input, Abort
// short-circuits when every request's abortRequestFor is nil (nothing
// to release). Refreshing the lease in those cases would be unsound:
// no quorum confirmation happened. We gate the refresh on
// resp.CommitIndex > 0, which the underlying manager sets to the
// last applied index only when at least one proposal went through.
type leaseRefreshingTxn struct {
	inner Transactional
	g     *ShardGroup
}

func (t *leaseRefreshingTxn) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	start := time.Now()
	expectedGen := t.g.lease.generation()
	resp, err := t.inner.Commit(reqs)
	if err != nil {
		// Only invalidate on errors that actually signal a leadership
		// change. Write-conflicts, validation errors, and deadline
		// exceeded on non-ReadIndex paths do NOT imply the leader is
		// gone; invalidating the lease for them forces every read
		// into the slow LinearizableRead path and defeats the whole
		// point of the lease. The engine's own leader-loss callback
		// already handles true leadership loss, plus
		// Coordinate.LeaseRead guards the fast path on
		// engine.State() == StateLeader.
		if isLeadershipLossError(err) {
			t.g.lease.invalidate()
		}
		return resp, errors.WithStack(err)
	}
	t.maybeRefresh(resp, start, expectedGen)
	return resp, nil
}

func (t *leaseRefreshingTxn) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	start := time.Now()
	expectedGen := t.g.lease.generation()
	resp, err := t.inner.Abort(reqs)
	if err != nil {
		if isLeadershipLossError(err) {
			t.g.lease.invalidate()
		}
		return resp, errors.WithStack(err)
	}
	t.maybeRefresh(resp, start, expectedGen)
	return resp, nil
}

// maybeRefresh extends the per-shard lease only when the operation
// actually produced a Raft commit. expectedGen is sampled BEFORE the
// underlying Commit/Abort so an invalidation that fires during that
// call observes a generation mismatch inside extend and the refresh
// is rejected. See the struct doc comment for why.
func (t *leaseRefreshingTxn) maybeRefresh(resp *TransactionResponse, start time.Time, expectedGen uint64) {
	if resp == nil || resp.CommitIndex == 0 {
		return
	}
	lp, ok := t.g.Engine.(raftengine.LeaseProvider)
	if !ok {
		return
	}
	t.g.lease.extend(start.Add(lp.LeaseDuration()), expectedGen)
}

// Close forwards to the wrapped Transactional if it implements
// io.Closer. ShardStore.closeGroup relies on the type assertion
// `g.Txn.(io.Closer)` to release per-shard resources (e.g. the gRPC
// connection cached by LeaderProxy). Without this pass-through, the
// wrapping would silently swallow the Closer capability and leak
// connections / goroutines at shutdown.
func (t *leaseRefreshingTxn) Close() error {
	closer, ok := t.inner.(io.Closer)
	if !ok {
		return nil
	}
	if err := closer.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
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
	// deregisterLeaseCbs removes the per-shard leader-loss callbacks
	// registered at construction. See Coordinate.Close for the
	// rationale.
	deregisterLeaseCbs []func()
	// leaseObserver records lease-read hit/miss for every shard the
	// coordinator owns. Nil-safe; see Coordinate.leaseObserver.
	leaseObserver LeaseReadObserver
}

// WithLeaseReadObserver wires a LeaseReadObserver onto a
// ShardedCoordinator. Applied after construction because the
// NewShardedCoordinator signature is already heavily overloaded;
// see Coordinate.WithLeaseReadObserver for the equivalent option on
// the single-group coordinator, including the typed-nil guard
// rationale.
func (c *ShardedCoordinator) WithLeaseReadObserver(observer LeaseReadObserver) *ShardedCoordinator {
	c.leaseObserver = normalizeLeaseObserver(observer)
	return c
}

// NewShardedCoordinator builds a coordinator for the provided shard groups.
// The defaultGroup is used for non-keyed leader checks.
func NewShardedCoordinator(engine *distribution.Engine, groups map[uint64]*ShardGroup, defaultGroup uint64, clock *HLC, st store.MVCCStore) *ShardedCoordinator {
	router := NewShardRouter(engine)
	var deregisters []func()
	for gid, g := range groups {
		// Wrap Txn so every successful Commit/Abort refreshes the
		// per-shard lease. Leave nil transactions unchanged, and skip
		// if already wrapped so repeat calls don't stack wrappers.
		if g.Txn != nil {
			if _, already := g.Txn.(*leaseRefreshingTxn); !already {
				g.Txn = &leaseRefreshingTxn{inner: g.Txn, g: g}
			}
		}
		router.Register(gid, g.Txn, g.Store)
		// Per-shard leader-loss hook: when this group's engine notices
		// a state transition out of leader, drop the lease so the next
		// LeaseReadForKey on that shard takes the slow path.
		if lp, ok := g.Engine.(raftengine.LeaseProvider); ok {
			deregisters = append(deregisters, lp.RegisterLeaderLossCallback(g.lease.invalidate))
		}
	}
	return &ShardedCoordinator{
		engine:             engine,
		router:             router,
		groups:             groups,
		defaultGroup:       defaultGroup,
		clock:              clock,
		store:              st,
		log:                slog.Default(),
		deregisterLeaseCbs: deregisters,
	}
}

// Close releases per-shard engine-side registrations. Idempotent.
func (c *ShardedCoordinator) Close() error {
	if c == nil {
		return nil
	}
	cbs := c.deregisterLeaseCbs
	c.deregisterLeaseCbs = nil
	for _, fn := range cbs {
		fn()
	}
	return nil
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
	if len(readKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
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

	if len(gids) == 1 && c.allReadKeysInShard(readKeys, gids[0]) {
		// Fast path: all mutations and read keys are in a single shard.
		// Use the one-phase path without allocating a grouped-read-keys map.
		// If any read key belongs to a different shard the 2PC path is required
		// so that validateReadOnlyShards can issue a linearizable read barrier,
		// preserving SSI.
		return c.dispatchSingleShardTxn(startTS, commitTS, primaryKey, gids[0], elems, readKeys)
	}

	// Multi-shard path: group read keys by shard now. The result is passed
	// directly to prewriteTxn to avoid a second iteration inside that function.
	groupedReadKeys := c.groupReadKeysByShardID(readKeys)
	prepared, err := c.prewriteTxn(ctx, startTS, commitTS, primaryKey, grouped, gids, groupedReadKeys)
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

// allReadKeysInShard returns true when every key in readKeys belongs to gid.
// It performs a single O(n) pass without allocating a map, making it suitable
// for the single-shard fast path in dispatchTxn.
func (c *ShardedCoordinator) allReadKeysInShard(readKeys [][]byte, gid uint64) bool {
	for _, rk := range readKeys {
		if c.engineGroupIDForKey(rk) != gid {
			return false
		}
	}
	return true
}

func (c *ShardedCoordinator) dispatchSingleShardTxn(startTS, commitTS uint64, primaryKey []byte, gid uint64, elems []*Elem[OP], readKeys [][]byte) (*CoordinateResponse, error) {
	g, err := c.txnGroupForID(gid)
	if err != nil {
		return nil, err
	}
	// ReadKeys are included in the Raft log entry so the FSM validates
	// read-write conflicts atomically under applyMu.
	resp, err := g.Txn.Commit([]*pb.Request{
		onePhaseTxnRequest(startTS, commitTS, primaryKey, elems, readKeys),
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

func (c *ShardedCoordinator) prewriteTxn(ctx context.Context, startTS, commitTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64, groupedReadKeys map[uint64][][]byte) ([]preparedGroup, error) {
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

func (c *ShardedCoordinator) RaftLeader() string {
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

func (c *ShardedCoordinator) RaftLeaderForKey(key []byte) string {
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

// LeaseRead routes through the default group's lease. See Coordinate.LeaseRead
// for semantics.
func (c *ShardedCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return groupLeaseRead(ctx, g, c.leaseObserver)
}

// LeaseReadForKey performs the lease check on the shard group that owns key.
// Each group maintains its own lease since each group has independent
// leadership and term.
func (c *ShardedCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	g, ok := c.groupForKey(key)
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return groupLeaseRead(ctx, g, c.leaseObserver)
}

// observeLeaseRead forwards a hit / miss signal to observer when it
// is non-nil. Kept as a package-level helper so both ShardedCoordinator
// and any future sharded caller share one nil-safe entrypoint.
func observeLeaseRead(observer LeaseReadObserver, hit bool) {
	if observer != nil {
		observer.ObserveLeaseRead(hit)
	}
}

func groupLeaseRead(ctx context.Context, g *ShardGroup, observer LeaseReadObserver) (uint64, error) {
	engine := engineForGroup(g)
	lp, ok := engine.(raftengine.LeaseProvider)
	if !ok {
		return linearizableReadEngineCtx(ctx, engine)
	}
	leaseDur := lp.LeaseDuration()
	if leaseDur <= 0 {
		return linearizableReadEngineCtx(ctx, engine)
	}
	// Single time.Now() sample so primary/secondary/extension all see
	// the same instant. Clock-skew safety delegated to
	// engineLeaseAckValid (see Coordinate.LeaseRead).
	now := time.Now()
	state := engine.State()
	if engineLeaseAckValid(state, lp.LastQuorumAck(), now, leaseDur) {
		observeLeaseRead(observer, true)
		return lp.AppliedIndex(), nil
	}
	expectedGen := g.lease.generation()
	if g.lease.valid(now) && state == raftengine.StateLeader {
		observeLeaseRead(observer, true)
		return lp.AppliedIndex(), nil
	}
	observeLeaseRead(observer, false)
	idx, err := linearizableReadEngineCtx(ctx, engine)
	if err != nil {
		if isLeadershipLossError(err) {
			g.lease.invalidate()
		}
		return 0, err
	}
	g.lease.extend(now.Add(leaseDur), expectedGen)
	return idx, nil
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
		if err := c.validateReadKeysOnShard(ctx, gid, keys, startTS); err != nil {
			return err
		}
	}
	return nil
}

func (c *ShardedCoordinator) validateReadKeysOnShard(ctx context.Context, gid uint64, keys [][]byte, startTS uint64) error {
	g, ok := c.groups[gid]
	if !ok {
		return nil
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

// RunHLCLeaseRenewal periodically proposes a new physical ceiling to the
// default shard group's Raft cluster while this node is the leader of that
// group. This mirrors the single-shard Coordinate.RunHLCLeaseRenewal behaviour,
// ensuring the shared HLC ceiling is maintained in multi-shard deployments.
//
// RunHLCLeaseRenewal blocks until ctx is cancelled; call it in a goroutine.
func (c *ShardedCoordinator) RunHLCLeaseRenewal(ctx context.Context) {
	group, ok := c.groups[c.defaultGroup]
	if !ok || group.Engine == nil {
		c.logger().WarnContext(ctx, "hlc lease renewal: default shard group not found or has no engine",
			slog.Uint64("default_group", c.defaultGroup),
		)
		return
	}
	// Use a Timer rather than a Ticker so the next renewal is scheduled
	// relative to the completion of the previous one. This prevents a burst
	// of back-to-back proposals if Propose stalls (e.g. waiting for Raft
	// quorum during a slow leader election).
	timer := time.NewTimer(hlcRenewalInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if group.Engine.State() == raftengine.StateLeader {
				ceilingMs := time.Now().UnixMilli() + hlcPhysicalWindowMs
				if _, err := group.Engine.Propose(ctx, marshalHLCLeaseRenew(ceilingMs)); err != nil {
					c.logger().WarnContext(ctx, "hlc lease renewal failed",
						slog.Uint64("group_id", c.defaultGroup),
						slog.Int64("ceiling_ms", ceilingMs),
						slog.Any("err", err),
					)
				}
			}
			timer.Reset(hlcRenewalInterval)
		case <-ctx.Done():
			return
		}
	}
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
