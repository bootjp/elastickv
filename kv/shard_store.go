package kv

import (
	"bytes"
	"context"
	"io"
	"sort"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

// ShardStore routes MVCC reads to shard-specific stores and proxies to leaders when needed.
type ShardStore struct {
	engine *distribution.Engine
	groups map[uint64]*ShardGroup

	connCache GRPCConnCache
}

var ErrCrossShardMutationBatchNotSupported = errors.New("cross-shard mutation batches are not supported")

// NewShardStore creates a sharded MVCC store wrapper.
func NewShardStore(engine *distribution.Engine, groups map[uint64]*ShardGroup) *ShardStore {
	return &ShardStore{
		engine: engine,
		groups: groups,
	}
}

func (s *ShardStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}

	// Some tests use ShardStore without raft; in that case serve reads locally.
	if g.Raft == nil {
		return s.localGetAt(ctx, g, key, ts)
	}

	// Verify leadership with a quorum before serving reads from local state to
	// avoid stale results from a deposed leader.
	if isVerifiedRaftLeader(g.Raft) {
		return s.leaderGetAt(ctx, g, key, ts)
	}
	return s.proxyRawGet(ctx, g, key, ts)
}

func isVerifiedRaftLeader(r *raft.Raft) bool {
	if r == nil || r.State() != raft.Leader {
		return false
	}
	return r.VerifyLeader().Error() == nil
}

func (s *ShardStore) leaderGetAt(ctx context.Context, g *ShardGroup, key []byte, ts uint64) ([]byte, error) {
	if !isTxnInternalKey(key) {
		if err := s.maybeResolveTxnLock(ctx, g, key, ts); err != nil {
			return nil, err
		}
	}
	return s.localGetAt(ctx, g, key, ts)
}

func (s *ShardStore) localGetAt(ctx context.Context, g *ShardGroup, key []byte, ts uint64) ([]byte, error) {
	val, err := g.Store.GetAt(ctx, key, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return val, nil
}

func (s *ShardStore) ExistsAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	v, err := s.GetAt(ctx, key, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return v != nil, nil
}

func (s *ShardStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	routes, clampToRoutes := s.routesForScan(start, end)
	out, err := s.scanRoutesAt(ctx, routes, start, end, limit, ts, clampToRoutes)
	if err != nil {
		return nil, err
	}

	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *ShardStore) routesForScan(start []byte, end []byte) ([]distribution.Route, bool) {
	// For internal list keys, shard routing is based on the logical user key
	// rather than the raw key prefix.
	if userKey := store.ExtractListUserKey(start); userKey != nil {
		route, ok := s.engine.GetRoute(userKey)
		if !ok {
			return []distribution.Route{}, false
		}
		return []distribution.Route{route}, false
	}

	routes := s.engine.GetIntersectingRoutes(start, end)
	// If the scan can include internal list keys (which use a fixed prefix),
	// avoid clamping to shard range bounds because those keys may be ordered
	// before the shard range start in raw keyspace.
	if len(start) == 0 {
		return routes, false
	}

	return routes, true
}

func (s *ShardStore) scanRoutesAt(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, clampToRoutes bool) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	for _, route := range routes {
		scanStart := start
		scanEnd := end
		if clampToRoutes {
			scanStart = clampScanStart(start, route.Start)
			scanEnd = clampScanEnd(end, route.End)
		}

		// Fetch up to 'limit' items from each shard. The final result will be
		// sorted and truncated by ScanAt.
		kvs, err := s.scanRouteAt(ctx, route, scanStart, scanEnd, limit, ts)
		if err != nil {
			return nil, err
		}
		out = append(out, kvs...)
	}
	return out, nil
}

func (s *ShardStore) scanRouteAt(ctx context.Context, route distribution.Route, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil, nil
	}

	if g.Raft == nil {
		kvs, err := g.Store.ScanAt(ctx, start, end, limit, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// Keep ScanAt behavior consistent even when running without raft.
		return filterTxnInternalKVs(kvs), nil
	}

	// Reads should come from the shard's leader to avoid returning stale or
	// incomplete results when this node is a follower for a given shard.
	if isVerifiedRaftLeader(g.Raft) {
		return s.scanRouteAtLeader(ctx, g, start, end, limit, ts)
	}

	kvs, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts)
	if err != nil {
		return nil, err
	}
	// The leader's RawScanAt is expected to perform lock resolution and filtering
	// via ShardStore.ScanAt, so avoid N+1 proxy gets here.
	return filterTxnInternalKVs(kvs), nil
}

func (s *ShardStore) scanRouteAtLeader(ctx context.Context, g *ShardGroup, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	kvs, err := g.Store.ScanAt(ctx, start, end, limit, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	lockEnd := scanLockUpperBoundForKVs(kvs, end)
	lockKVs, err := scanTxnLockRangeAt(ctx, g, start, lockEnd, ts)
	if err != nil {
		return nil, err
	}
	return s.resolveScanLocks(ctx, g, kvs, lockKVs, ts)
}

func scanLockUpperBoundForKVs(kvs []*store.KVPair, scanEnd []byte) []byte {
	lastUserKey := lastNonInternalScanKey(kvs)
	if len(lastUserKey) == 0 {
		return scanEnd
	}
	bound := nextScanCursor(lastUserKey)
	if scanEnd == nil || bytes.Compare(bound, scanEnd) < 0 {
		return bound
	}
	return scanEnd
}

func lastNonInternalScanKey(kvs []*store.KVPair) []byte {
	for i := len(kvs) - 1; i >= 0; i-- {
		kvp := kvs[i]
		if kvp == nil || isTxnInternalKey(kvp.Key) {
			continue
		}
		return kvp.Key
	}
	return nil
}

func (s *ShardStore) groupForID(groupID uint64) (*ShardGroup, bool) {
	g, ok := s.groups[groupID]
	return g, ok
}

func clampScanStart(start []byte, routeStart []byte) []byte {
	if start == nil {
		return routeStart
	}
	if bytes.Compare(start, routeStart) < 0 {
		return routeStart
	}
	return start
}

func clampScanEnd(end []byte, routeEnd []byte) []byte {
	if routeEnd == nil {
		return end
	}
	if end == nil {
		return routeEnd
	}
	if bytes.Compare(end, routeEnd) > 0 {
		return routeEnd
	}
	return end
}

func (s *ShardStore) PutAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return store.ErrNotSupported
	}
	return errors.WithStack(g.Store.PutAt(ctx, key, value, commitTS, expireAt))
}

func (s *ShardStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return store.ErrNotSupported
	}
	return errors.WithStack(g.Store.DeleteAt(ctx, key, commitTS))
}

func (s *ShardStore) PutWithTTLAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return store.ErrNotSupported
	}
	return errors.WithStack(g.Store.PutWithTTLAt(ctx, key, value, commitTS, expireAt))
}

func (s *ShardStore) ExpireAt(ctx context.Context, key []byte, expireAt uint64, commitTS uint64) error {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return store.ErrNotSupported
	}
	return errors.WithStack(g.Store.ExpireAt(ctx, key, expireAt, commitTS))
}

func (s *ShardStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return 0, false, nil
	}

	if g.Raft == nil {
		ts, exists, err := g.Store.LatestCommitTS(ctx, key)
		if err != nil {
			return 0, false, errors.WithStack(err)
		}
		return ts, exists, nil
	}

	// Avoid returning a stale watermark when our local raft instance is a
	// deposed leader.
	if g.Raft.State() == raft.Leader {
		if err := g.Raft.VerifyLeader().Error(); err == nil {
			ts, exists, err := g.Store.LatestCommitTS(ctx, key)
			if err != nil {
				return 0, false, errors.WithStack(err)
			}
			return ts, exists, nil
		}
	}

	return s.proxyLatestCommitTS(ctx, g, key)
}

func (s *ShardStore) proxyLatestCommitTS(ctx context.Context, g *ShardGroup, key []byte) (uint64, bool, error) {
	if g == nil || g.Raft == nil {
		return 0, false, nil
	}
	addr, _ := g.Raft.LeaderWithID()
	if addr == "" {
		return 0, false, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return 0, false, err
	}

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: key})
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return resp.Ts, resp.Exists, nil
}

func (s *ShardStore) maybeResolveTxnLock(ctx context.Context, g *ShardGroup, key []byte, readTS uint64) error {
	lock, ok, err := loadTxnLockAt(ctx, g, key, readTS)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return s.resolveTxnLockForKey(ctx, g, key, lock)
}

func loadTxnLockAt(ctx context.Context, g *ShardGroup, key []byte, ts uint64) (txnLock, bool, error) {
	if g == nil || g.Store == nil {
		return txnLock{}, false, nil
	}
	// Only consider locks visible at the provided read timestamp.
	lockBytes, err := g.Store.GetAt(ctx, txnLockKey(key), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return txnLock{}, false, nil
		}
		return txnLock{}, false, errors.WithStack(err)
	}
	lock, err := decodeTxnLock(lockBytes)
	if err != nil {
		return txnLock{}, false, errors.WithStack(err)
	}
	return lock, true, nil
}

func (s *ShardStore) resolveTxnLockForKey(ctx context.Context, g *ShardGroup, key []byte, lock txnLock) error {
	// Check primary transaction status to decide commit/rollback.
	status, commitTS, err := s.primaryTxnStatus(ctx, lock.PrimaryKey, lock.StartTS)
	if err != nil {
		return err
	}
	switch status {
	case txnStatusCommitted:
		return applyTxnResolution(g, pb.Phase_COMMIT, lock.StartTS, commitTS, lock.PrimaryKey, [][]byte{key})
	case txnStatusRolledBack:
		abortTS := abortTSFrom(lock.StartTS, lock.StartTS)
		if abortTS <= lock.StartTS {
			// Overflow: can't choose an abort timestamp strictly greater than startTS.
			return errors.Wrapf(ErrTxnLocked, "key: %s (timestamp overflow)", string(key))
		}
		return applyTxnResolution(g, pb.Phase_ABORT, lock.StartTS, abortTS, lock.PrimaryKey, [][]byte{key})
	case txnStatusPending:
		return errors.Wrapf(ErrTxnLocked, "key: %s", string(key))
	default:
		return errors.Wrapf(ErrTxnInvalidMeta, "unknown txn status for key %s", string(key))
	}
}

type scanItem struct {
	kvp    *store.KVPair
	skip   bool
	locked bool
}

type lockTxnKey struct {
	startTS uint64
	primary string
}

type lockTxnStatus struct {
	status   txnStatus
	commitTS uint64
}

type lockResolutionBatch struct {
	phase      pb.Phase
	startTS    uint64
	resolveTS  uint64
	primaryKey []byte
	keys       [][]byte
	seen       map[string]struct{}
}

type scanLockPlan struct {
	items             []scanItem
	itemIndex         map[string]int
	statusCache       map[lockTxnKey]lockTxnStatus
	resolutionBatches map[lockTxnKey]*lockResolutionBatch
	batchOrder        []lockTxnKey
}

func newScanLockPlan(size int) *scanLockPlan {
	return &scanLockPlan{
		items:             make([]scanItem, 0, size),
		itemIndex:         make(map[string]int, size),
		statusCache:       make(map[lockTxnKey]lockTxnStatus),
		resolutionBatches: make(map[lockTxnKey]*lockResolutionBatch),
		batchOrder:        make([]lockTxnKey, 0),
	}
}

func (s *ShardStore) resolveScanLocks(ctx context.Context, g *ShardGroup, kvs []*store.KVPair, lockKVs []*store.KVPair, ts uint64) ([]*store.KVPair, error) {
	if len(kvs) == 0 && len(lockKVs) == 0 {
		return kvs, nil
	}
	if g == nil || g.Store == nil {
		return []*store.KVPair{}, nil
	}

	plan, err := s.planScanLockResolutions(ctx, g, kvs, lockKVs, ts)
	if err != nil {
		return nil, err
	}
	if err := applyScanLockResolutions(g, plan); err != nil {
		return nil, err
	}
	return s.materializeScanLockResults(ctx, g, ts, plan.items)
}

func (s *ShardStore) planScanLockResolutions(ctx context.Context, g *ShardGroup, kvs []*store.KVPair, lockKVs []*store.KVPair, ts uint64) (*scanLockPlan, error) {
	plan := newScanLockPlan(len(kvs) + len(lockKVs))
	for _, kvp := range lockKVs {
		if err := s.planScanLockFromLockKVP(ctx, plan, kvp); err != nil {
			return nil, err
		}
	}
	for _, kvp := range kvs {
		if err := s.planScanLockItem(ctx, g, ts, plan, kvp); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func (s *ShardStore) planScanLockFromLockKVP(ctx context.Context, plan *scanLockPlan, kvp *store.KVPair) error {
	if kvp == nil {
		return nil
	}
	userKey, ok := txnUserKeyFromLockKey(kvp.Key)
	if !ok {
		return nil
	}

	lock, err := decodeTxnLock(kvp.Value)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(lock.PrimaryKey) == 0 {
		return errors.Wrapf(ErrTxnInvalidMeta, "missing txn primary key for key %s", string(userKey))
	}

	return s.planLockedUserKey(ctx, plan, userKey, lock)
}

func (s *ShardStore) planScanLockItem(ctx context.Context, g *ShardGroup, ts uint64, plan *scanLockPlan, kvp *store.KVPair) error {
	if kvp == nil || isTxnInternalKey(kvp.Key) {
		plan.items = append(plan.items, scanItem{skip: true})
		return nil
	}
	if _, exists := plan.itemIndex[string(kvp.Key)]; exists {
		return nil
	}

	lock, locked, err := loadTxnLockAt(ctx, g, kvp.Key, ts)
	if err != nil {
		return err
	}
	if !locked {
		appendScanItem(plan, kvp, false)
		return nil
	}
	return s.planLockedUserKey(ctx, plan, kvp.Key, lock)
}

func (s *ShardStore) planLockedUserKey(ctx context.Context, plan *scanLockPlan, userKey []byte, lock txnLock) error {
	if len(lock.PrimaryKey) == 0 {
		return errors.Wrapf(ErrTxnInvalidMeta, "missing txn primary key for key %s", string(userKey))
	}

	txnKey := lockTxnKey{startTS: lock.StartTS, primary: string(lock.PrimaryKey)}
	state, err := s.cachedLockTxnStatus(ctx, plan, lock, txnKey)
	if err != nil {
		return err
	}
	phase, resolveTS, err := lockResolutionForStatus(state, lock, userKey)
	if err != nil {
		return err
	}
	appendScanLockResolutionBatch(plan, txnKey, phase, resolveTS, lock, userKey)
	appendScanItem(plan, &store.KVPair{Key: userKey}, true)
	return nil
}

func (s *ShardStore) cachedLockTxnStatus(ctx context.Context, plan *scanLockPlan, lock txnLock, txnKey lockTxnKey) (lockTxnStatus, error) {
	if state, ok := plan.statusCache[txnKey]; ok {
		return state, nil
	}
	status, commitTS, err := s.primaryTxnStatus(ctx, lock.PrimaryKey, lock.StartTS)
	if err != nil {
		return lockTxnStatus{}, err
	}
	state := lockTxnStatus{status: status, commitTS: commitTS}
	plan.statusCache[txnKey] = state
	return state, nil
}

func lockResolutionForStatus(state lockTxnStatus, lock txnLock, key []byte) (pb.Phase, uint64, error) {
	switch state.status {
	case txnStatusPending:
		return pb.Phase_NONE, 0, errors.Wrapf(ErrTxnLocked, "key: %s", string(key))
	case txnStatusCommitted:
		return pb.Phase_COMMIT, state.commitTS, nil
	case txnStatusRolledBack:
		abortTS := abortTSFrom(lock.StartTS, lock.StartTS)
		if abortTS <= lock.StartTS {
			// Overflow: can't choose an abort timestamp strictly greater than startTS.
			return pb.Phase_NONE, 0, errors.Wrapf(ErrTxnLocked, "key: %s (timestamp overflow)", string(key))
		}
		return pb.Phase_ABORT, abortTS, nil
	default:
		return pb.Phase_NONE, 0, errors.Wrapf(ErrTxnInvalidMeta, "unknown txn status for key %s", string(key))
	}
}

func appendScanLockResolutionBatch(plan *scanLockPlan, txnKey lockTxnKey, phase pb.Phase, resolveTS uint64, lock txnLock, key []byte) {
	batch, exists := plan.resolutionBatches[txnKey]
	if !exists {
		batch = &lockResolutionBatch{
			phase:      phase,
			startTS:    lock.StartTS,
			resolveTS:  resolveTS,
			primaryKey: lock.PrimaryKey,
			keys:       make([][]byte, 0, 1),
			seen:       map[string]struct{}{},
		}
		plan.resolutionBatches[txnKey] = batch
		plan.batchOrder = append(plan.batchOrder, txnKey)
	}
	keyID := string(key)
	if _, duplicated := batch.seen[keyID]; duplicated {
		return
	}
	batch.seen[keyID] = struct{}{}
	batch.keys = append(batch.keys, key)
}

func appendScanItem(plan *scanLockPlan, kvp *store.KVPair, locked bool) {
	if kvp == nil || len(kvp.Key) == 0 {
		return
	}
	keyID := string(kvp.Key)
	if idx, exists := plan.itemIndex[keyID]; exists {
		if locked {
			plan.items[idx].locked = true
		}
		return
	}

	plan.itemIndex[keyID] = len(plan.items)
	plan.items = append(plan.items, scanItem{kvp: kvp, locked: locked})
}

func txnUserKeyFromLockKey(lockKey []byte) ([]byte, bool) {
	if !bytes.HasPrefix(lockKey, []byte(txnLockPrefix)) {
		return nil, false
	}
	return bytes.Clone(lockKey[len(txnLockPrefix):]), true
}

func scanTxnLockRangeAt(ctx context.Context, g *ShardGroup, start []byte, end []byte, ts uint64) ([]*store.KVPair, error) {
	if g == nil || g.Store == nil {
		return []*store.KVPair{}, nil
	}

	lockStart, lockEnd := txnLockScanBounds(start, end)
	return scanTxnLockPagesAt(ctx, g.Store, lockStart, lockEnd, ts)
}

func scanTxnLockPagesAt(ctx context.Context, st store.MVCCStore, start []byte, end []byte, ts uint64) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	cursor := start
	for {
		lockKVs, nextCursor, done, err := scanTxnLockPageAt(ctx, st, cursor, end, ts)
		if err != nil {
			return nil, err
		}
		out = append(out, lockKVs...)
		if done {
			return out, nil
		}
		cursor = nextCursor
	}
}

func scanTxnLockPageAt(ctx context.Context, st store.MVCCStore, start []byte, end []byte, ts uint64) ([]*store.KVPair, []byte, bool, error) {
	const lockPageLimit = 256

	lockKVs, err := st.ScanAt(ctx, start, end, lockPageLimit, ts)
	if err != nil {
		return nil, nil, false, errors.WithStack(err)
	}
	if len(lockKVs) == 0 || len(lockKVs) < lockPageLimit {
		return lockKVs, nil, true, nil
	}

	last := lockKVs[len(lockKVs)-1]
	if last == nil || len(last.Key) == 0 {
		return lockKVs, nil, true, nil
	}

	nextCursor := nextScanCursor(last.Key)
	if end != nil && bytes.Compare(nextCursor, end) >= 0 {
		return lockKVs, nil, true, nil
	}

	return lockKVs, nextCursor, false, nil
}

func nextScanCursor(lastKey []byte) []byte {
	next := make([]byte, len(lastKey)+1)
	copy(next, lastKey)
	return next
}

func txnLockScanBounds(start []byte, end []byte) ([]byte, []byte) {
	lockStart := txnLockKey(start)
	if end != nil {
		return lockStart, txnLockKey(end)
	}
	return lockStart, prefixScanEnd([]byte(txnLockPrefix))
}

func prefixScanEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	out := bytes.Clone(prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == ^byte(0) {
			continue
		}
		out[i]++
		return out[:i+1]
	}
	return nil
}

func applyScanLockResolutions(g *ShardGroup, plan *scanLockPlan) error {
	for _, txnKey := range plan.batchOrder {
		batch := plan.resolutionBatches[txnKey]
		if err := applyTxnResolution(g, batch.phase, batch.startTS, batch.resolveTS, batch.primaryKey, batch.keys); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) materializeScanLockResults(ctx context.Context, g *ShardGroup, ts uint64, items []scanItem) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0, len(items))
	for _, item := range items {
		if item.skip {
			continue
		}
		if !item.locked {
			out = append(out, item.kvp)
			continue
		}
		v, err := s.localGetAt(ctx, g, item.kvp.Key, ts)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, &store.KVPair{Key: item.kvp.Key, Value: v})
	}
	return out, nil
}

func filterTxnInternalKVs(kvs []*store.KVPair) []*store.KVPair {
	if len(kvs) == 0 {
		return kvs
	}
	out := make([]*store.KVPair, 0, len(kvs))
	for _, kvp := range kvs {
		if kvp == nil {
			continue
		}
		if isTxnInternalKey(kvp.Key) {
			continue
		}
		out = append(out, kvp)
	}
	return out
}

type txnStatus int

const (
	txnStatusPending txnStatus = iota
	txnStatusCommitted
	txnStatusRolledBack
)

func (s *ShardStore) primaryTxnStatus(ctx context.Context, primaryKey []byte, startTS uint64) (txnStatus, uint64, error) {
	status, commitTS, done, err := s.primaryTxnRecordedStatus(ctx, primaryKey, startTS)
	if err != nil || done {
		return status, commitTS, err
	}

	lock, locked, err := s.primaryTxnLock(ctx, primaryKey, startTS)
	if err != nil {
		return txnStatusPending, 0, err
	}
	if !locked {
		return txnStatusRolledBack, 0, nil
	}
	if !txnLockExpired(lock) {
		return txnStatusPending, 0, nil
	}
	return s.expiredPrimaryTxnStatus(ctx, primaryKey, startTS)
}

func (s *ShardStore) primaryTxnRecordedStatus(ctx context.Context, primaryKey []byte, startTS uint64) (txnStatus, uint64, bool, error) {
	commitTS, committed, err := s.txnCommitTS(ctx, primaryKey, startTS)
	if err != nil {
		return txnStatusPending, 0, false, err
	}
	if committed {
		return txnStatusCommitted, commitTS, true, nil
	}

	rolledBack, err := s.hasTxnRollback(ctx, primaryKey, startTS)
	if err != nil {
		return txnStatusPending, 0, false, err
	}
	if rolledBack {
		return txnStatusRolledBack, 0, true, nil
	}
	return txnStatusPending, 0, false, nil
}

func (s *ShardStore) primaryTxnLock(ctx context.Context, primaryKey []byte, startTS uint64) (txnLock, bool, error) {
	lock, ok, err := s.loadTxnLock(ctx, primaryKey)
	if err != nil {
		return txnLock{}, false, err
	}
	if !ok || lock.StartTS != startTS {
		return txnLock{}, false, nil
	}
	return lock, true, nil
}

func txnLockExpired(lock txnLock) bool {
	return lock.TTLExpireAt != 0 && hlcWallNow() > lock.TTLExpireAt
}

func (s *ShardStore) expiredPrimaryTxnStatus(ctx context.Context, primaryKey []byte, startTS uint64) (txnStatus, uint64, error) {
	aborted, err := s.tryAbortExpiredPrimary(primaryKey, startTS)
	if err != nil {
		return s.statusAfterAbortFailure(ctx, primaryKey, startTS)
	}
	if aborted {
		return txnStatusRolledBack, 0, nil
	}
	return txnStatusPending, 0, nil
}

func (s *ShardStore) statusAfterAbortFailure(ctx context.Context, primaryKey []byte, startTS uint64) (txnStatus, uint64, error) {
	if commitTS, committed, err := s.txnCommitTS(ctx, primaryKey, startTS); err == nil && committed {
		return txnStatusCommitted, commitTS, nil
	}
	if rolledBack, err := s.hasTxnRollback(ctx, primaryKey, startTS); err == nil && rolledBack {
		return txnStatusRolledBack, 0, nil
	}
	// Keep reads conservative when timeout cleanup cannot be confirmed.
	return txnStatusPending, 0, nil
}

func (s *ShardStore) txnCommitTS(ctx context.Context, primaryKey []byte, startTS uint64) (uint64, bool, error) {
	b, err := s.GetAt(ctx, txnCommitKey(primaryKey, startTS), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	cts, derr := decodeTxnCommitRecord(b)
	if derr != nil {
		return 0, false, errors.WithStack(derr)
	}
	return cts, true, nil
}

func (s *ShardStore) hasTxnRollback(ctx context.Context, primaryKey []byte, startTS uint64) (bool, error) {
	_, err := s.GetAt(ctx, txnRollbackKey(primaryKey, startTS), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *ShardStore) loadTxnLock(ctx context.Context, primaryKey []byte) (txnLock, bool, error) {
	lockBytes, err := s.GetAt(ctx, txnLockKey(primaryKey), ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return txnLock{}, false, nil
		}
		return txnLock{}, false, err
	}
	lock, derr := decodeTxnLock(lockBytes)
	if derr != nil {
		return txnLock{}, false, errors.WithStack(derr)
	}
	return lock, true, nil
}

func (s *ShardStore) tryAbortExpiredPrimary(primaryKey []byte, startTS uint64) (bool, error) {
	pg, ok := s.groupForKey(primaryKey)
	if !ok || pg == nil || pg.Txn == nil {
		return false, nil
	}
	abortTS := abortTSFrom(startTS, startTS)
	if abortTS <= startTS {
		// Overflow: can't choose an abort timestamp strictly greater than startTS.
		return false, nil
	}
	if err := applyTxnResolution(pg, pb.Phase_ABORT, startTS, abortTS, primaryKey, [][]byte{primaryKey}); err != nil {
		return false, err
	}
	return true, nil
}

func applyTxnResolution(g *ShardGroup, phase pb.Phase, startTS, commitTS uint64, primaryKey []byte, keys [][]byte) error {
	if g == nil || g.Txn == nil {
		return errors.WithStack(store.ErrNotSupported)
	}
	meta := &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, CommitTS: commitTS}),
	}
	muts := make([]*pb.Mutation, 0, len(keys)+1)
	muts = append(muts, meta)
	for _, k := range keys {
		muts = append(muts, &pb.Mutation{Op: pb.Op_PUT, Key: k})
	}
	_, err := g.Txn.Commit([]*pb.Request{{IsTxn: true, Phase: phase, Ts: startTS, Mutations: muts}})
	return errors.WithStack(err)
}



// ApplyMutations applies a batch of mutations to the correct shard store.
//
// All mutations must belong to the same shard. Cross-shard mutation batches are
// not supported.
func (s *ShardStore) ApplyMutations(ctx context.Context, mutations []*store.KVPairMutation, startTS, commitTS uint64) error {
	if len(mutations) == 0 {
		return nil
	}
	// Determine the shard group for the first mutation.
	firstGroup, ok := s.groupForKey(mutations[0].Key)
	if !ok || firstGroup == nil || firstGroup.Store == nil {
		return store.ErrNotSupported
	}
	// Ensure that all mutations in the batch belong to the same shard.
	for i := 1; i < len(mutations); i++ {
		g, ok := s.groupForKey(mutations[i].Key)
		if !ok || g == nil || g.Store == nil {
			return store.ErrNotSupported
		}
		if g != firstGroup {
			return errors.WithStack(ErrCrossShardMutationBatchNotSupported)
		}
	}
	return errors.WithStack(firstGroup.Store.ApplyMutations(ctx, mutations, startTS, commitTS))
}

func (s *ShardStore) LastCommitTS() uint64 {
	var max uint64
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		if ts := g.Store.LastCommitTS(); ts > max {
			max = ts
		}
	}
	return max
}

func (s *ShardStore) Compact(ctx context.Context, minTS uint64) error {
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		if err := g.Store.Compact(ctx, minTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *ShardStore) Snapshot() (io.ReadWriter, error) {
	return nil, store.ErrNotSupported
}

func (s *ShardStore) Restore(_ io.Reader) error {
	return store.ErrNotSupported
}

func (s *ShardStore) Close() error {
	var first error
	for _, g := range s.groups {
		if err := s.closeGroup(g); err != nil && first == nil {
			first = err
		}
	}

	if err := s.connCache.Close(); err != nil && first == nil {
		first = err
	}

	return first
}

func (s *ShardStore) closeGroup(g *ShardGroup) error {
	if g == nil {
		return nil
	}

	var first error
	if g.Store != nil {
		if err := g.Store.Close(); err != nil && first == nil {
			first = errors.WithStack(err)
		}
	}

	if closer, ok := g.Txn.(io.Closer); ok {
		if err := closer.Close(); err != nil && first == nil {
			first = errors.WithStack(err)
		}
	}

	return first
}

func (s *ShardStore) groupForKey(key []byte) (*ShardGroup, bool) {
	route, ok := s.engine.GetRoute(routeKey(key))
	if !ok {
		return nil, false
	}
	g, ok := s.groups[route.GroupID]
	return g, ok
}

func (s *ShardStore) proxyRawGet(ctx context.Context, g *ShardGroup, key []byte, ts uint64) ([]byte, error) {
	if g == nil || g.Raft == nil {
		return nil, store.ErrKeyNotFound
	}
	addr, _ := g.Raft.LeaderWithID()
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Compatibility with older nodes that don't set RawGetResponse.exists:
	// treat any non-nil payload as found even when exists=false.
	if !resp.GetExists() && resp.GetValue() == nil {
		return nil, store.ErrKeyNotFound
	}
	return resp.Value, nil
}

func (s *ShardStore) proxyRawScanAt(ctx context.Context, g *ShardGroup, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if g == nil || g.Raft == nil {
		return nil, store.ErrNotSupported
	}
	addr, _ := g.Raft.LeaderWithID()
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: start,
		EndKey:   end,
		Limit:    int64(limit),
		Ts:       ts,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]*store.KVPair, 0, len(resp.Kv))
	for _, kvp := range resp.Kv {
		out = append(out, &store.KVPair{
			Key:   bytes.Clone(kvp.Key),
			Value: bytes.Clone(kvp.Value),
		})
	}

	return out, nil
}

var _ store.MVCCStore = (*ShardStore)(nil)
