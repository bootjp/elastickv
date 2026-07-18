package kv

import (
	"bytes"
	"context"
	"io"
	"sort"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	proxyForwardTimeout          = 5 * time.Second
	readRouteVersionWaitTimeout  = 200 * time.Millisecond
	readRouteVersionPollInterval = 2 * time.Millisecond
)

// ShardStore routes MVCC reads to shard-specific stores and proxies to leaders when needed.
type ShardStore struct {
	engine *distribution.Engine
	groups map[uint64]*ShardGroup

	connCache GRPCConnCache
}

var (
	ErrCrossShardMutationBatchNotSupported     = errors.New("cross-shard mutation batches are not supported")
	ErrRouteCutoverPending                     = errors.New("route cutover pending")
	ErrExplicitGroupStagedVisibilityUnresolved = errors.New("explicit group read cannot resolve staged visibility route")
	ErrReadRouteVersionUnavailable             = errors.New("read route version is not locally available")
	ErrRouteWriteBelowFloor                    = ErrRouteWriteTimestampTooLow
)

// NewShardStore creates a sharded MVCC store wrapper.
func NewShardStore(engine *distribution.Engine, groups map[uint64]*ShardGroup) *ShardStore {
	return &ShardStore{
		engine: engine,
		groups: groups,
	}
}

func (s *ShardStore) ReadRouteVersion() uint64 {
	if s == nil || s.engine == nil {
		return 0
	}
	return s.engine.Version()
}

func (s *ShardStore) awaitReadRouteVersion(ctx context.Context, requested uint64) error {
	if requested == 0 || s.ReadRouteVersion() >= requested {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, readRouteVersionWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(readRouteVersionPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			return errors.Wrapf(ErrReadRouteVersionUnavailable, "requested=%d current=%d: %v", requested, s.ReadRouteVersion(), waitCtx.Err())
		case <-ticker.C:
			if s.ReadRouteVersion() >= requested {
				return nil
			}
		}
	}
}

func (s *ShardStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	return s.GetAtWithReadFence(ctx, key, ts, 0, 0)
}

func (s *ShardStore) GetAtWithReadFence(ctx context.Context, key []byte, ts uint64, groupID uint64, readRouteVersion uint64) ([]byte, error) {
	if err := s.awaitReadRouteVersion(ctx, readRouteVersion); err != nil {
		return nil, err
	}
	if groupID != 0 {
		return s.getGroupAtWithReadFence(ctx, groupID, key, ts, readRouteVersion)
	}
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}

	// Some tests use ShardStore without raft; in that case serve reads locally.
	if engineForGroup(g) == nil {
		return s.localGetAt(ctx, g, route, key, ts)
	}

	// Wait for a leader read fence before serving from local state.
	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.leaderGetAt(ctx, g, route, key, ts)
	}
	return s.proxyRawGet(ctx, g, key, ts, 0, readRouteVersion)
}

// GetGroupAt reads a key from the explicitly selected Raft group.
// It is for keyspaces whose owner is resolved outside the byte-range
// engine (for example SQS HT-FIFO's (queue, partition) resolver).
func (s *ShardStore) GetGroupAt(ctx context.Context, groupID uint64, key []byte, ts uint64) ([]byte, error) {
	return s.getGroupAtWithReadFence(ctx, groupID, key, ts, 0)
}

func (s *ShardStore) getGroupAtWithReadFence(ctx context.Context, groupID uint64, key []byte, ts uint64, readRouteVersion uint64) ([]byte, error) {
	g, ok := s.groupForID(groupID)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}
	route, err := s.routeForExplicitGroupKey(groupID, key)
	if err != nil {
		return nil, err
	}

	return s.getGroupAt(ctx, g, route, key, ts, groupID, readRouteVersion)
}

func (s *ShardStore) getRouteAt(ctx context.Context, route distribution.Route, key []byte, ts uint64) ([]byte, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}
	return s.getGroupAt(ctx, g, route, key, ts, route.GroupID, 0)
}

func (s *ShardStore) getGroupAt(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte, ts uint64, groupID uint64, readRouteVersion uint64) ([]byte, error) {
	if engineForGroup(g) == nil {
		return s.localGetAt(ctx, g, route, key, ts)
	}
	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.leaderGetAt(ctx, g, route, key, ts)
	}
	return s.proxyRawGet(ctx, g, key, ts, groupID, readRouteVersion)
}

func isLinearizableRaftLeader(ctx context.Context, engine raftengine.LeaderView) bool {
	if !isLeaderEngine(engine) {
		return false
	}
	// Lease-aware fence: when the engine's quorum-ack lease is fresh,
	// leaseReadEngineCtx returns the current AppliedIndex without
	// issuing a new read-index request. Previously this path always
	// called LinearizableRead per GetAt, which funnelled every
	// in-script redis.call through the single raft dispatch worker
	// and starved heartbeats under sustained Lua load. The lease
	// guarantees no concurrent leader exists within LeaseDuration,
	// so the local applied index is still safe to serve; the slow
	// read-index is only paid on lease miss.
	_, err := leaseReadEngineCtx(ctx, engine)
	return err == nil
}

func (s *ShardStore) leaderGetAt(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte, ts uint64) ([]byte, error) {
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return nil, err
	}
	if !isTxnInternalKey(key) {
		if err := s.maybeResolveTxnLock(ctx, g, key, ts); err != nil {
			return nil, err
		}
	}
	return s.localGetAt(ctx, g, route, key, ts)
}

func (s *ShardStore) localGetAt(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte, ts uint64) ([]byte, error) {
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return nil, err
	}
	if routeHasStagedVisibility(route) {
		return s.getAtWithStagedVisibility(ctx, g, route, key, ts)
	}
	val, err := g.Store.GetAt(ctx, key, ts)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return val, nil
}

func routeHasStagedVisibility(route distribution.Route) bool {
	return route.StagedVisibilityActive && route.MigrationJobID != 0
}

func routeSatisfiesTargetReadiness(route distribution.Route, ready store.TargetStagedReadinessState, catalogVersion uint64) bool {
	if !routeRangeIntersects(route.Start, route.End, ready.RouteStart, ready.RouteEnd) {
		return false
	}
	if route.MinWriteTSExclusive < ready.MinWriteTSExclusive {
		return false
	}
	if route.StagedVisibilityActive {
		return route.MigrationJobID == ready.MigrationJobID
	}
	if route.MigrationJobID != 0 {
		return false
	}
	return ready.ExpectedCutoverVersion == 0 || catalogVersion >= ready.ExpectedCutoverVersion
}

func (s *ShardStore) targetReadyRouteForRange(ctx context.Context, g *ShardGroup, route distribution.Route, start []byte, end []byte) (distribution.Route, error) {
	routeStart, routeEnd := readinessRouteRangeForScan(start, end)
	return s.targetReadyRouteForRouteRange(ctx, g, route, routeStart, routeEnd)
}

func (s *ShardStore) verifyTargetReadinessForRange(ctx context.Context, g *ShardGroup, route distribution.Route, start []byte, end []byte) error {
	_, err := s.targetReadyRouteForRange(ctx, g, route, start, end)
	return err
}

func (s *ShardStore) targetReadyRouteForRouteRange(ctx context.Context, g *ShardGroup, route distribution.Route, routeStart []byte, routeEnd []byte) (distribution.Route, error) {
	routes, err := s.targetReadyRoutesForRouteRange(ctx, g, route, routeStart, routeEnd)
	if err != nil {
		return route, err
	}
	if len(routes) == 1 {
		return routes[0], nil
	}
	return route, nil
}

func (s *ShardStore) targetReadyRoutesForRouteRange(ctx context.Context, g *ShardGroup, route distribution.Route, routeStart []byte, routeEnd []byte) ([]distribution.Route, error) {
	if g == nil || g.Store == nil {
		return []distribution.Route{route}, nil
	}
	reader, ok := g.Store.(store.MigrationTargetReadinessReader)
	if !ok {
		return []distribution.Route{route}, nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceReadFenceApplies(states, routeStart, routeEnd) {
		return nil, errors.WithStack(ErrRouteCutoverPending)
	}
	applicable := targetReadinessApplicableStates(states, route, routeStart, routeEnd)
	if len(applicable) == 0 {
		return []distribution.Route{route}, nil
	}

	proofRoutes, catalogVersion, ok := s.readinessProofRoutes(route, routeStart, routeEnd)
	if !ok {
		return nil, errors.WithStack(ErrRouteCutoverPending)
	}
	if !readinessProofSatisfiesStates(applicable, proofRoutes, route.GroupID, catalogVersion) {
		return nil, errors.WithStack(ErrRouteCutoverPending)
	}
	return proofRoutes, nil
}

func targetReadinessApplicableStates(
	states []store.TargetStagedReadinessState,
	route distribution.Route,
	routeStart []byte,
	routeEnd []byte,
) []store.TargetStagedReadinessState {
	applicable := make([]store.TargetStagedReadinessState, 0, len(states))
	for _, ready := range states {
		if ready.SourceWriteFence || ready.SourceReadFence || ready.TrackWrites {
			continue
		}
		if targetReadinessAppliesToRoute(route, routeStart, routeEnd, ready) {
			applicable = append(applicable, ready)
		}
	}
	return applicable
}

func sourceReadFenceApplies(states []store.TargetStagedReadinessState, routeStart []byte, routeEnd []byte) bool {
	for _, state := range states {
		if state.Armed && state.SourceReadFence && routeRangeIntersects(routeStart, routeEnd, state.RouteStart, state.RouteEnd) {
			return true
		}
	}
	return false
}

func readinessProofSatisfiesStates(
	states []store.TargetStagedReadinessState,
	routes []distribution.Route,
	groupID uint64,
	catalogVersion uint64,
) bool {
	for _, ready := range states {
		if !routesSatisfyTargetReadiness(routes, ready, groupID, catalogVersion) {
			return false
		}
	}
	return true
}

func (s *ShardStore) verifyTargetReadinessForRouteRange(ctx context.Context, g *ShardGroup, route distribution.Route, routeStart []byte, routeEnd []byte) error {
	_, err := s.targetReadyRouteForRouteRange(ctx, g, route, routeStart, routeEnd)
	return err
}

func (s *ShardStore) readinessProofRoutes(route distribution.Route, routeStart []byte, routeEnd []byte) ([]distribution.Route, uint64, bool) {
	if s == nil || s.engine == nil {
		return []distribution.Route{route}, 0, true
	}
	snap, ok := s.engine.Current()
	if !ok {
		return nil, 0, false
	}
	routes := snap.IntersectingRoutes(routeStart, routeEnd)
	proof := routes[:0]
	for _, candidate := range routes {
		if candidate.GroupID != route.GroupID {
			continue
		}
		if (route.Start != nil || route.End != nil || route.RouteID != 0) &&
			!routeRangeIntersects(candidate.Start, candidate.End, route.Start, route.End) {
			continue
		}
		proof = append(proof, candidate)
	}
	return proof, snap.Version(), len(proof) > 0
}

func targetReadinessAppliesToRoute(route distribution.Route, routeStart []byte, routeEnd []byte, ready store.TargetStagedReadinessState) bool {
	if !ready.Armed || !routeRangeIntersects(routeStart, routeEnd, ready.RouteStart, ready.RouteEnd) {
		return false
	}
	if route.RouteID != 0 && !routeRangeIntersects(route.Start, route.End, ready.RouteStart, ready.RouteEnd) {
		return false
	}
	return true
}

func readinessRouteRange(start []byte, end []byte) ([]byte, []byte) {
	if routeStart, routeEnd, ok := s3BucketAuxiliaryRouteRange(start); ok && (end == nil || bytes.Equal(end, nextScanCursor(start))) {
		return routeStart, routeEnd
	}
	routeStart := routeKey(start)
	if end == nil {
		return routeStart, nil
	}
	routeEnd := routeKey(end)
	if bytes.Compare(routeEnd, routeStart) <= 0 {
		routeEnd = nextScanCursor(routeStart)
	}
	return routeStart, routeEnd
}

func readinessRouteRangeForScan(start []byte, end []byte) ([]byte, []byte) {
	if routeStart, routeEnd, ok := s3keys.ManifestScanRouteBounds(start, end); ok {
		return routeStart, routeEnd
	}
	return readinessRouteRange(start, end)
}

func verifyRouteWriteFloor(route distribution.Route, commitTS uint64) error {
	if route.MinWriteTSExclusive == 0 || commitTS == 0 || commitTS > route.MinWriteTSExclusive {
		return nil
	}
	return errors.Wrapf(ErrRouteWriteBelowFloor, "commit_ts %d <= floor %d", commitTS, route.MinWriteTSExclusive)
}

func routeRangeIntersects(aStart, aEnd, bStart, bEnd []byte) bool {
	if aEnd != nil && bytes.Compare(aEnd, bStart) <= 0 {
		return false
	}
	if bEnd != nil && bytes.Compare(bEnd, aStart) <= 0 {
		return false
	}
	return true
}

func (s *ShardStore) routeForExplicitGroupKey(groupID uint64, key []byte) (distribution.Route, error) {
	fallback := distribution.Route{GroupID: groupID}
	if s == nil || s.engine == nil {
		return fallback, nil
	}
	if route, ok := s.stagedVisibilityRouteForS3BucketAuxiliaryKey(key); ok {
		if route.GroupID == groupID {
			return route, nil
		}
		return distribution.Route{}, errors.Wrapf(ErrExplicitGroupStagedVisibilityUnresolved, "group_id=%d key=%q", groupID, key)
	}
	if route, ok := s.engine.GetRoute(routeKey(key)); ok {
		if route.GroupID == groupID {
			return route, nil
		}
		if routeHasStagedVisibility(route) {
			return distribution.Route{}, errors.Wrapf(ErrExplicitGroupStagedVisibilityUnresolved, "group_id=%d key=%q", groupID, key)
		}
	}
	return fallback, nil
}

func (s *ShardStore) getAtWithStagedVisibility(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte, ts uint64) ([]byte, error) {
	if err := ensureReadTSRetained(g.Store, ts); err != nil {
		return nil, err
	}
	live, liveOK, err := latestMVCCVersionAt(ctx, g.Store, key, ts)
	if err != nil {
		return nil, err
	}
	stagedKey := distribution.MigrationStagedDataKey(route.MigrationJobID, key)
	staged, stagedOK, err := latestMVCCVersionAt(ctx, g.Store, stagedKey, ts)
	if err != nil {
		return nil, err
	}
	if stagedOK {
		staged.Key = bytes.Clone(key)
	}
	winner, ok := newerMigrationVersion(live, liveOK, staged, stagedOK)
	if !ok || !migrationVersionVisible(winner, ts) {
		return nil, store.ErrKeyNotFound
	}
	return bytes.Clone(winner.Value), nil
}

func latestMVCCVersionAt(ctx context.Context, st store.MVCCStore, key []byte, ts uint64) (store.MVCCVersion, bool, error) {
	opts := store.ExportVersionsOptions{
		StartKey:             key,
		EndKey:               prefixScanEnd(key),
		MaxCommitTSInclusive: ts,
		ReadTS:               ts,
		MaxVersions:          1,
		MaxScannedBytes:      0,
		MinCommitTSExclusive: 0,
		MaxBytes:             0,
		KeyFamily:            0,
		AcceptKey: func(rawKey []byte) bool {
			return bytes.Equal(rawKey, key)
		},
	}
	for {
		result, err := st.ExportVersions(ctx, opts)
		if err != nil {
			return store.MVCCVersion{}, false, errors.WithStack(err)
		}
		for _, version := range result.Versions {
			if bytes.Equal(version.Key, key) {
				return version, true, nil
			}
		}
		if result.Done || len(result.NextCursor) == 0 {
			return store.MVCCVersion{}, false, nil
		}
		opts.Cursor = result.NextCursor
	}
}

func newerMigrationVersion(a store.MVCCVersion, aOK bool, b store.MVCCVersion, bOK bool) (store.MVCCVersion, bool) {
	switch {
	case !aOK:
		return b, bOK
	case !bOK:
		return a, true
	case b.CommitTS >= a.CommitTS:
		return b, true
	default:
		return a, true
	}
}

func migrationVersionVisible(version store.MVCCVersion, ts uint64) bool {
	return !version.Tombstone && (version.ExpireAt == 0 || version.ExpireAt > ts)
}

func ensureReadTSRetained(st store.MVCCStore, ts uint64) error {
	retention, ok := st.(store.RetentionController)
	if !ok {
		return nil
	}
	minRetainedTS := retention.MinRetainedTS()
	if minRetainedTS != 0 && ts != 0 && ts != ^uint64(0) && ts < minRetainedTS {
		return errors.WithStack(store.ErrReadTSCompacted)
	}
	return nil
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

// CommittedVersionAt routes the exact-timestamp existence probe to the
// owning group's local store, gated on the same lease-aware leader check
// GetAt uses, so a deposed node that has not yet applied a freshly-
// committed entry does not silently return false to a client read. The
// FSM apply path is NOT affected — it holds the per-shard store directly
// (not ShardStore) and runs the probe on the deterministic local replica
// it is writing to. The option-2 reuse path (RedisServer.resolveReuseLength)
// goes through this wrapper, so during leader churn the probe must answer
// authoritatively or defer to a leader-routed re-read.
//
// There is no RawCommittedVersionAt RPC to proxy to; when we are not the
// linearizable leader for the group we return (false, nil) and let the
// caller fall back to derived reads (resolveListMeta uses ScanAt/GetAt,
// which ARE leader-fenced / proxied per group). The fallback returns the
// leader's current Len — a valid serialization — at the cost of the
// pending.length fast-path during churn. Mirrors LeaderRoutedStore's fix
// for codex P1 #796.
func (s *ShardStore) CommittedVersionAt(ctx context.Context, key []byte, commitTS uint64) (bool, error) {
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g.Store == nil {
		return false, nil
	}
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return false, err
	}
	// engineForGroup may be nil in test fixtures that wire ShardStore
	// without raft; preserve the existing local-only fallback there.
	engine := engineForGroup(g)
	if engine == nil {
		return committedVersionAtForRoute(ctx, g.Store, route, key, commitTS)
	}
	if !isLinearizableRaftLeader(ctx, engine) && !tryEngineLinearizableFence(ctx, engine) {
		// Not the linearizable leader for this group AND the ReadIndex
		// fence failed (no leader reachable, ctx canceled). Fall back to
		// (false, nil); the adapter's resolveListMeta path takes over via
		// the leader-fenced ScanAt/GetAt and returns a valid current-Len
		// serialization.
		return false, nil
	}
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return false, err
	}
	return committedVersionAtForRoute(ctx, g.Store, route, key, commitTS)
}

func committedVersionAtForRoute(ctx context.Context, st store.MVCCStore, route distribution.Route, key []byte, commitTS uint64) (bool, error) {
	exists, err := st.CommittedVersionAt(ctx, key, commitTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if exists || !routeHasStagedVisibility(route) {
		return exists, nil
	}
	stagedKey := distribution.MigrationStagedDataKey(route.MigrationJobID, key)
	exists, err = st.CommittedVersionAt(ctx, stagedKey, commitTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return exists, nil
}

// tryEngineLinearizableFence submits a Raft ReadIndex via the per-group
// engine and reports whether it succeeded. After a successful ReadIndex
// the local applied index is caught up to the current leader's commit
// point, so a subsequent local read sees every committed version. The
// error from the underlying call is intentionally not surfaced — callers
// that need the authoritative answer treat a failed fence as "couldn't
// verify, fall back to the leader-routed slow path." Structured to avoid
// the nilerr false positive at the call site.
func tryEngineLinearizableFence(ctx context.Context, engine raftengine.LeaderView) bool {
	if engine == nil {
		return false
	}
	_, err := engine.LinearizableRead(ctx)
	return err == nil
}

// ScanAt scans keys across shards at the given timestamp. When the caller has
// already fenced every group to applied_index >= f(ts), as BeginBackup does,
// the result is consistent across groups. Without that fence, ranges spanning
// multiple shards are best-effort because each shard may have a different Raft
// apply position.
func (s *ShardStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	return s.scanAtWithReadFence(ctx, start, end, limit, ts, 0, s.ReadRouteVersion(), nil, nil)
}

func (s *ShardStore) ScanAtWithReadFence(ctx context.Context, start []byte, end []byte, limit int, ts uint64, reverse bool, groupID uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	if err := s.awaitReadRouteVersion(ctx, readRouteVersion); err != nil {
		return nil, err
	}
	if reverse {
		if groupID != 0 {
			if routeScanBoundsPresent(routeStart, routeEnd) {
				return s.scanExplicitGroupAtWithReadFence(ctx, groupID, start, end, limit, ts, true, readRouteVersion, routeStart, routeEnd)
			}
			return nil, errors.WithStack(store.ErrNotSupported)
		}
		return s.reverseScanAtWithReadFence(ctx, start, end, limit, ts, readRouteVersion, routeStart, routeEnd)
	}
	return s.scanAtWithReadFence(ctx, start, end, limit, ts, groupID, readRouteVersion, routeStart, routeEnd)
}

func (s *ShardStore) scanAtWithReadFence(ctx context.Context, start []byte, end []byte, limit int, ts uint64, groupID uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	if groupID != 0 {
		return s.scanExplicitGroupAtWithReadFence(ctx, groupID, start, end, limit, ts, false, readRouteVersion, routeStart, routeEnd)
	}

	routes, clampToRoutes := s.routesForFencedScan(start, end, routeStart, routeEnd)
	out, err := s.scanRoutesAtWithReadFence(ctx, routes, start, end, limit, ts, clampToRoutes, readRouteVersion, routeStart, routeEnd)
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

func (s *ShardStore) ScanKeysAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	if limit <= 0 {
		return [][]byte{}, nil
	}

	routes, clampToRoutes := s.routesForScan(start, end)
	out, err := s.scanKeyRoutesAt(ctx, routes, start, end, limit, ts, clampToRoutes)
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i], out[j]) < 0
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *ShardStore) ScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*store.KVPair, bool, error) {
	if visibleLimit <= 0 || physicalLimit <= 0 {
		return []*store.KVPair{}, false, nil
	}
	routes, clampToRoutes := s.routesForScan(start, end)
	if routesContainStagedVisibility(routes) {
		kvs, err := s.ScanAt(ctx, start, end, visibleLimit, ts)
		return kvs, false, err
	}
	if len(routes) != 1 || clampToRoutes {
		kvs, err := s.ScanAt(ctx, start, end, visibleLimit, ts)
		return kvs, false, err
	}
	return s.scanRouteAtDirectionPhysicalLimit(ctx, routes[0], start, end, visibleLimit, physicalLimit, ts, false)
}

// ScanGroupAt scans a range on the explicitly selected Raft group.
// It is for keyspaces whose owner is resolved outside the byte-range
// engine (for example SQS HT-FIFO's (queue, partition) resolver).
// Normal callers should use ScanAt so range scans keep following the
// distribution engine's route table.
func (s *ShardStore) ScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	return s.scanExplicitGroupAtWithReadFence(ctx, groupID, start, end, limit, ts, false, 0, nil, nil)
}

func (s *ShardStore) scanExplicitGroupAtWithReadFence(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64, reverse bool, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	routes, clampToRoutes, err := s.routesForExplicitGroupScanWithRouteBounds(groupID, start, end, routeStart, routeEnd)
	if err != nil {
		return nil, err
	}
	readinessStart, readinessEnd := readinessRouteRangeForScan(start, end)
	if err := s.verifyExplicitGroupRoutesForRange(ctx, groupID, routes, readinessStart, readinessEnd); err != nil {
		return nil, err
	}
	routeFilterPresent := routeScanBoundsPresent(routeStart, routeEnd)
	dedupeByKey := s3BucketAuxiliaryScanBounds(start, end)
	if !clampToRoutes && !routeFilterPresent {
		routes, dedupeByKey = prepareUnclampedRawScanRoutes(routes, dedupeByKey)
	}
	return s.scanExplicitGroupRoutesAtWithReadFence(ctx, routes, start, end, limit, ts, reverse, readRouteVersion, routeStart, routeEnd, clampToRoutes, dedupeByKey)
}

func (s *ShardStore) scanExplicitGroupRoutesAtWithReadFence(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, reverse bool, readRouteVersion uint64, routeStart []byte, routeEnd []byte, clampToRoutes bool, dedupeByKey bool) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	for i := 0; i < len(routes); i++ {
		route := routes[i]
		if reverse && clampToRoutes {
			route = routes[len(routes)-1-i]
		}
		scanStart := start
		scanEnd := end
		if clampToRoutes {
			scanStart = clampScanStart(start, route.Start)
			scanEnd = clampScanEnd(end, route.End)
		}
		kvs, err := s.scanRouteAtDirectionWithReadFence(ctx, route, scanStart, scanEnd, limit, ts, reverse, true, readRouteVersion, routeStart, routeEnd)
		if err != nil {
			return nil, err
		}
		if clampToRoutes {
			out = append(out, kvs...)
			if len(out) >= limit {
				clear(out[limit:])
				return out[:limit], nil
			}
			continue
		}
		out = mergeAndTrimScanResultsWithOptions(out, kvs, limit, reverse, dedupeByKey)
	}
	return out, nil
}

// ScanGroupKeysAt scans keys on the explicitly selected Raft group without
// materializing values over proxy links.
func (s *ShardStore) ScanGroupKeysAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	if limit <= 0 {
		return [][]byte{}, nil
	}
	return s.scanKeyRouteAt(ctx, distribution.Route{GroupID: groupID}, start, end, limit, ts)
}

func (s *ShardStore) ReverseScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	return s.reverseScanAtWithReadFence(ctx, start, end, limit, ts, s.ReadRouteVersion(), nil, nil)
}

func (s *ShardStore) reverseScanAtWithReadFence(ctx context.Context, start []byte, end []byte, limit int, ts uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	routes, clampToRoutes := s.routesForFencedScan(start, end, routeStart, routeEnd)
	out, err := s.reverseScanRoutesAtWithReadFence(ctx, routes, start, end, limit, ts, clampToRoutes, readRouteVersion, routeStart, routeEnd)
	if err != nil {
		return nil, err
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *ShardStore) ReverseScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*store.KVPair, bool, error) {
	if visibleLimit <= 0 || physicalLimit <= 0 {
		return []*store.KVPair{}, false, nil
	}
	routes, clampToRoutes := s.routesForScan(start, end)
	if routesContainStagedVisibility(routes) {
		kvs, err := s.ReverseScanAt(ctx, start, end, visibleLimit, ts)
		return kvs, false, err
	}
	if len(routes) != 1 || clampToRoutes {
		kvs, err := s.ReverseScanAt(ctx, start, end, visibleLimit, ts)
		return kvs, false, err
	}
	return s.scanRouteAtDirectionPhysicalLimit(ctx, routes[0], start, end, visibleLimit, physicalLimit, ts, true)
}

func (s *ShardStore) AllowExactScanFallbackAfterPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, _ uint64, _ bool) bool {
	if visibleLimit <= 0 || physicalLimit <= 0 {
		return false
	}
	g := s.exactFallbackPhysicalLimitGroup(start, end)
	if g == nil {
		return false
	}
	if _, ok := g.Store.(physicalLimitedStore); !ok {
		return false
	}
	engine := engineForGroup(g)
	return engine == nil || isLinearizableRaftLeader(ctx, engine)
}

func (s *ShardStore) exactFallbackPhysicalLimitGroup(start []byte, end []byte) *ShardGroup {
	if s == nil || s.engine == nil {
		return nil
	}
	routes, clampToRoutes := s.routesForScan(start, end)
	if len(routes) != 1 || clampToRoutes {
		return nil
	}
	g, ok := s.groupForID(routes[0].GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil
	}
	return g
}

func (s *ShardStore) routesForScan(start []byte, end []byte) ([]distribution.Route, bool) {
	if routeStart, routeEnd, ok := s3keys.ManifestScanRouteBounds(start, end); ok {
		return s.engine.GetIntersectingRoutes(routeStart, routeEnd), false
	}
	if routes, ok := s.routesForS3BucketAuxiliaryScan(start, end); ok {
		return routes, false
	}
	if isBroadLegacyListDeltaScan(start) {
		return s.engine.GetIntersectingRoutes(nil, nil), false
	}
	if routes, ok := s.routesForLegacyListDeltaScan(start, end); ok {
		return routes, false
	}
	// For internal wide-column keys, shard routing is based on the logical
	// user key rather than the raw key prefix.
	if userKey := scanRouteUserKey(start); userKey != nil {
		route, ok := s.engine.GetRoute(userKey)
		if !ok {
			return []distribution.Route{}, false
		}
		return []distribution.Route{route}, false
	}
	if userKey := redisWideColumnScanRouteKey(start); userKey != nil {
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

func (s *ShardStore) routesForS3BucketAuxiliaryScan(start []byte, end []byte) ([]distribution.Route, bool) {
	if s == nil || s.engine == nil || !s3BucketAuxiliaryScanBounds(start, end) {
		return nil, false
	}
	routes := make([]distribution.Route, 0)
	routes = append(routes, s.engine.GetIntersectingRoutes(start, end)...)
	routeStart, routeEnd := s3BucketAuxiliaryScanRouteRange(start, end)
	for _, route := range s.engine.GetIntersectingRoutes(routeStart, routeEnd) {
		if routeHasStagedVisibility(route) {
			routes = append(routes, route)
		}
	}
	return routes, true
}

func s3BucketAuxiliaryScanBounds(start []byte, end []byte) bool {
	if !bytes.HasPrefix(start, []byte(s3keys.BucketMetaPrefix)) &&
		!bytes.HasPrefix(start, []byte(s3keys.BucketGenerationPrefix)) {
		return false
	}
	if end == nil {
		return true
	}
	return bytes.Compare(start, end) < 0
}

func s3BucketAuxiliaryScanRouteRange(start []byte, end []byte) ([]byte, []byte) {
	if routeStart, routeEnd, ok := s3BucketAuxiliaryRouteRange(start); ok && end != nil && bytes.Compare(end, prefixScanEnd(start)) <= 0 {
		return routeStart, routeEnd
	}
	routeStart := []byte(s3keys.RoutePrefix)
	return routeStart, prefixScanEnd(routeStart)
}

type repeatedRawScanRouteKey struct {
	groupID        uint64
	staged         bool
	migrationJobID uint64
	routeStart     string
	routeEnd       string
}

func dedupeRepeatedRawScanRoutes(routes []distribution.Route) []distribution.Route {
	if len(routes) <= 1 {
		return routes
	}
	out := make([]distribution.Route, 0, len(routes))
	seen := make(map[repeatedRawScanRouteKey]struct{}, len(routes))
	for _, route := range routes {
		key := repeatedRawScanRouteDedupeKey(route)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, route)
	}
	return out
}

func repeatedRawScanRouteDedupeKey(route distribution.Route) repeatedRawScanRouteKey {
	key := repeatedRawScanRouteKey{groupID: route.GroupID}
	if routeHasStagedVisibility(route) {
		key.staged = true
		key.migrationJobID = route.MigrationJobID
		key.routeStart = string(route.Start)
		key.routeEnd = string(route.End)
	}
	return key
}

func prepareUnclampedRawScanRoutes(routes []distribution.Route, dedupeByKey bool) ([]distribution.Route, bool) {
	if routesContainStagedVisibility(routes) {
		routes = dedupeRepeatedRawScanRoutes(routes)
		return orderRawScanRoutesForStagedVisibility(routes), true
	}
	if !dedupeByKey {
		routes = dedupeRepeatedRawScanRoutes(routes)
	}
	return routes, dedupeByKey
}

func orderRawScanRoutesForStagedVisibility(routes []distribution.Route) []distribution.Route {
	if !routesContainStagedVisibility(routes) {
		return routes
	}
	out := make([]distribution.Route, 0, len(routes))
	staged := make([]distribution.Route, 0)
	for _, route := range routes {
		if routeHasStagedVisibility(route) {
			staged = append(staged, route)
			continue
		}
		out = append(out, route)
	}
	return append(out, staged...)
}

func routesContainStagedVisibility(routes []distribution.Route) bool {
	for _, route := range routes {
		if routeHasStagedVisibility(route) {
			return true
		}
	}
	return false
}

func (s *ShardStore) routesForExplicitGroupScanWithRouteBounds(groupID uint64, start []byte, end []byte, routeStart []byte, routeEnd []byte) ([]distribution.Route, bool, error) {
	if routeScanBoundsPresent(routeStart, routeEnd) {
		return s.routesForExplicitGroupRouteBounds(groupID, start, end, routeStart, routeEnd)
	}
	return s.routesForExplicitGroupScan(groupID, start, end)
}

func (s *ShardStore) routesForExplicitGroupRouteBounds(groupID uint64, start []byte, end []byte, routeStart []byte, routeEnd []byte) ([]distribution.Route, bool, error) {
	fallback := []distribution.Route{{GroupID: groupID}}
	if s == nil || s.engine == nil {
		return fallback, false, nil
	}
	routes := s.engine.GetIntersectingRoutes(routeStart, normalizedRouteScanEnd(routeEnd))
	matched := make([]distribution.Route, 0, len(routes))
	for _, route := range routes {
		if route.GroupID == groupID {
			matched = append(matched, route)
			continue
		}
		if routeHasStagedVisibility(route) {
			return nil, false, errors.Wrapf(ErrExplicitGroupStagedVisibilityUnresolved, "group_id=%d range=[%q,%q)", groupID, start, end)
		}
	}
	if len(matched) == 0 {
		return fallback, false, nil
	}
	return matched, false, nil
}

func (s *ShardStore) routesForExplicitGroupScan(groupID uint64, start []byte, end []byte) ([]distribution.Route, bool, error) {
	fallback := []distribution.Route{{GroupID: groupID}}
	if s == nil || s.engine == nil {
		return fallback, false, nil
	}
	routeStart, routeEnd, routeMapped := explicitGroupScanRouteBounds(start, end)
	routes := s.engine.GetIntersectingRoutes(routeStart, routeEnd)
	matched := make([]distribution.Route, 0, len(routes))
	for _, route := range routes {
		if route.GroupID == groupID {
			matched = append(matched, route)
			continue
		}
		if routeHasStagedVisibility(route) {
			return nil, false, errors.Wrapf(ErrExplicitGroupStagedVisibilityUnresolved, "group_id=%d range=[%q,%q)", groupID, start, end)
		}
	}
	if len(matched) > 0 {
		if routeMapped {
			matched = dedupeRepeatedRawScanRoutes(matched)
		}
		return matched, !routeMapped, nil
	}
	return fallback, false, nil
}

func explicitGroupScanRouteBounds(start []byte, end []byte) ([]byte, []byte, bool) {
	routeStart := routeKey(start)
	if len(start) == 0 {
		routeStart = []byte("")
	}
	routeEnd := end
	routeMapped := !bytes.Equal(routeStart, start)
	if end != nil {
		normalizedEnd := routeKey(end)
		if !bytes.Equal(normalizedEnd, end) {
			routeMapped = true
		}
	}
	if routeMapped && len(routeStart) != 0 {
		routeEnd = prefixScanEnd(routeStart)
	}
	return routeStart, routeEnd, routeMapped
}

func (s *ShardStore) routesForLegacyListDeltaScan(start []byte, end []byte) ([]distribution.Route, bool) {
	logicalUserKey := store.ExtractLegacyListUserKeyFromDeltaScanPrefix(start)
	if logicalUserKey == nil {
		return nil, false
	}
	routes := make([]distribution.Route, 0)
	if route, ok := s.engine.GetRoute(logicalUserKey); ok {
		routes = append(routes, route)
	}
	storedStart := store.ExtractListUserKey(start)
	if storedStart != nil {
		var storedEnd []byte
		if len(end) > 0 {
			storedEnd = store.ExtractListUserKey(end)
		}
		routes = append(routes, s.engine.GetIntersectingRoutes(storedStart, storedEnd)...)
	}
	return routes, true
}

func isBroadLegacyListDeltaScan(start []byte) bool {
	prefix := []byte(store.LegacyListMetaDeltaPrefix)
	if !bytes.HasPrefix(start, prefix) {
		return false
	}
	logicalUserKey := store.ExtractLegacyListUserKeyFromDeltaScanPrefix(start)
	return logicalUserKey == nil || !bytes.Equal(start, store.LegacyListMetaDeltaScanPrefix(logicalUserKey))
}

func shouldMarkRouteGroupOnScan(start []byte, explicitGroup bool, routeStart []byte, routeEnd []byte) bool {
	return !explicitGroup && !routeScanBoundsPresent(routeStart, routeEnd) && isBroadLegacyListDeltaScan(start)
}

func scanRouteUserKey(start []byte) []byte {
	for _, extract := range scanRouteUserKeyExtractors {
		if userKey := extract(start); userKey != nil {
			return userKey
		}
	}
	return nil
}

var scanRouteUserKeyExtractors = []func([]byte) []byte{
	store.ExtractListUserKeyFromDeltaScanPrefix,
	store.ExtractListUserKey,
	store.ExtractListUserKeyFromClaimScanPrefix,
	store.ExtractHashUserKeyFromField,
	store.ExtractHashUserKeyFromDeltaScanPrefix,
	store.ExtractSetUserKeyFromMember,
	store.ExtractSetUserKeyFromDeltaScanPrefix,
	store.ExtractZSetUserKeyFromMember,
	store.ExtractZSetUserKeyFromScore,
	store.ExtractZSetUserKeyFromScoreScanPrefix,
	store.ExtractZSetUserKeyFromDeltaScanPrefix,
	store.ExtractStreamUserKeyFromMeta,
	store.ExtractStreamUserKeyFromEntryScanPrefix,
}

func (s *ShardStore) routesForFencedScan(start []byte, end []byte, routeStart []byte, routeEnd []byte) ([]distribution.Route, bool) {
	if routeScanBoundsPresent(routeStart, routeEnd) {
		return s.engine.GetIntersectingRoutes(routeStart, normalizedRouteScanEnd(routeEnd)), false
	}
	return s.routesForScan(start, end)
}

func routeScanBoundsPresent(routeStart []byte, routeEnd []byte) bool {
	return routeStart != nil || routeEnd != nil
}

func normalizedRouteScanEnd(routeEnd []byte) []byte {
	if len(routeEnd) == 0 {
		return nil
	}
	return routeEnd
}

func (s *ShardStore) scanRoutesAtWithReadFence(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, clampToRoutes bool, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	routeFilterPresent := routeScanBoundsPresent(routeStart, routeEnd)
	dedupeByKey := s3BucketAuxiliaryScanBounds(start, end)
	if !clampToRoutes && !routeFilterPresent {
		routes, dedupeByKey = prepareUnclampedRawScanRoutes(routes, dedupeByKey)
	}
	for _, route := range routes {
		scanStart := start
		scanEnd := end
		if clampToRoutes {
			scanStart = clampScanStart(start, route.Start)
			scanEnd = clampScanEnd(end, route.End)
		}

		kvs, err := s.scanRouteAtDirectionWithReadFence(ctx, route, scanStart, scanEnd, limit, ts, false, false, readRouteVersion, routeStart, routeEnd)
		if err != nil {
			return nil, err
		}
		if clampToRoutes {
			out = append(out, kvs...)
			if len(out) >= limit {
				out = out[:limit]
				break
			}
			continue
		}
		out = mergeAndTrimScanResultsWithOptions(out, kvs, limit, false, dedupeByKey)
	}
	return out, nil
}

func (s *ShardStore) scanKeyRoutesAt(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, clampToRoutes bool) ([][]byte, error) {
	out := make([][]byte, 0)
	seenGroups := make(map[uint64]struct{})
	for _, route := range routes {
		scanStart := start
		scanEnd := end
		if clampToRoutes {
			scanStart = clampScanStart(start, route.Start)
			scanEnd = clampScanEnd(end, route.End)
		} else {
			if _, seen := seenGroups[route.GroupID]; seen {
				continue
			}
			seenGroups[route.GroupID] = struct{}{}
		}

		keys, err := s.scanKeyRouteAt(ctx, route, scanStart, scanEnd, limit, ts)
		if err != nil {
			return nil, err
		}
		if clampToRoutes {
			out = mergeAndTrimScanKeys(out, keys, limit)
			if len(out) >= limit {
				break
			}
			continue
		}
		out = mergeAndTrimScanKeys(out, keys, limit)
	}
	return out, nil
}

func (s *ShardStore) reverseScanRoutesAtWithReadFence(
	ctx context.Context,
	routes []distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	clampToRoutes bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	routeFilterPresent := routeScanBoundsPresent(routeStart, routeEnd)
	dedupeByKey := s3BucketAuxiliaryScanBounds(start, end)
	if !clampToRoutes && !routeFilterPresent {
		routes, dedupeByKey = prepareUnclampedRawScanRoutes(routes, dedupeByKey)
	}
	for i := 0; i < len(routes); i++ {
		route := routes[i]
		if clampToRoutes {
			route = routes[len(routes)-1-i]
			kvs, done, err := s.clampedReverseScanRouteAtWithReadFence(ctx, route, start, end, limit, len(out), ts, readRouteVersion, routeStart, routeEnd)
			if err != nil {
				return nil, err
			}
			if done {
				break
			}
			out = append(out, kvs...)
			continue
		}

		// When clampToRoutes is false (e.g. S3 manifest scans spanning multiple
		// shards), keys from different routes may interleave in descending order.
		// Fetch up to limit from every route and merge+sort descending so the
		// result honours the ReverseScanAt contract.
		kvs, err := s.scanRouteAtDirectionWithReadFence(ctx, route, start, end, limit, ts, true, false, readRouteVersion, routeStart, routeEnd)
		if err != nil {
			return nil, err
		}
		out = mergeAndTrimScanResultsWithOptions(out, kvs, limit, true, dedupeByKey)
	}
	return out, nil
}

func (s *ShardStore) scanKeyRouteAt(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([][]byte, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil, nil
	}

	if engineForGroup(g) == nil {
		return s.scanKeysRouteLocal(ctx, g, start, end, limit, ts)
	}

	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.scanKeysRouteAtLeader(ctx, g, route, start, end, limit, ts)
	}

	return s.proxyScanKeysAt(ctx, g, start, end, limit, ts, route.GroupID)
}

func (s *ShardStore) scanKeysRouteLocal(
	ctx context.Context,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([][]byte, error) {
	return scanKeysWithRefill(start, end, limit, func(cursor []byte, pageLimit int) ([][]byte, error) {
		keys, err := g.Store.ScanKeysAt(ctx, cursor, end, pageLimit, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return keys, nil
	})
}

func (s *ShardStore) scanKeysRouteAtLeader(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) ([][]byte, error) {
	if limit <= 0 {
		return [][]byte{}, nil
	}

	out := make([][]byte, 0, limit)
	cursor := start
	for len(out) < limit {
		keys, err := g.Store.ScanKeysAt(ctx, cursor, end, limit, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(keys) == 0 {
			keys, err := s.scanLockOnlyKeysAtLeader(ctx, g, route, cursor, end, ts, limit)
			if err != nil {
				return nil, err
			}
			out = mergeAndTrimScanKeys(out, keys, limit)
			break
		}

		keyKVs := kvPairsFromKeys(keys)
		lockStart, lockEnd := scanLockBoundsForKVs(keyKVs, cursor, end, limit)
		lockKVs, err := scanTxnLockRangeAt(ctx, g, lockStart, lockEnd, ts, limit)
		if err != nil {
			return nil, err
		}
		kvs, err := s.resolveScanLocks(ctx, g, route, keyKVs, lockKVs, ts)
		if err != nil {
			return nil, err
		}
		out = mergeAndTrimScanKeys(out, filterTxnInternalKeys(keysFromKVs(kvs)), limit)

		nextCursor, ok := nextKeyScanCursor(keys, end, limit)
		if !ok {
			break
		}
		cursor = nextCursor
	}
	return out, nil
}

func (s *ShardStore) scanLockOnlyKeysAtLeader(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	ts uint64,
	limit int,
) ([][]byte, error) {
	lockKVs, err := scanTxnLockRangeAt(ctx, g, start, end, ts, limit)
	if err != nil {
		return nil, err
	}
	if len(lockKVs) == 0 {
		return nil, nil
	}
	kvs, err := s.resolveScanLocks(ctx, g, route, nil, lockKVs, ts)
	if err != nil {
		return nil, err
	}
	return filterTxnInternalKeys(keysFromKVs(kvs)), nil
}

func (s *ShardStore) proxyScanKeysAt(
	ctx context.Context,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	groupID uint64,
) ([][]byte, error) {
	readRouteVersion := s.ReadRouteVersion()
	return scanKeysWithRefill(start, end, limit, func(cursor []byte, pageLimit int) ([][]byte, error) {
		return s.proxyRawScanKeysAt(ctx, g, cursor, end, pageLimit, ts, groupID, readRouteVersion)
	})
}

func scanKeysWithRefill(
	start []byte,
	end []byte,
	limit int,
	scan func(cursor []byte, pageLimit int) ([][]byte, error),
) ([][]byte, error) {
	if limit <= 0 {
		return [][]byte{}, nil
	}

	out := make([][]byte, 0, limit)
	cursor := start
	for len(out) < limit {
		keys, err := scan(cursor, limit)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			break
		}

		out = mergeAndTrimScanKeys(out, filterTxnInternalKeys(keys), limit)

		nextCursor, ok := nextKeyScanCursor(keys, end, limit)
		if !ok {
			break
		}
		cursor = nextCursor
	}
	return out, nil
}

func nextKeyScanCursor(keys [][]byte, end []byte, limit int) ([]byte, bool) {
	lastKey := lastScanKey(keys)
	if len(keys) < limit || lastKey == nil {
		return nil, false
	}
	nextCursor := nextScanCursor(lastKey)
	if end != nil && bytes.Compare(nextCursor, end) >= 0 {
		return nil, false
	}
	return nextCursor, true
}

func lastScanKey(keys [][]byte) []byte {
	for i := len(keys) - 1; i >= 0; i-- {
		if keys[i] != nil {
			return keys[i]
		}
	}
	return nil
}

func (s *ShardStore) clampedReverseScanRouteAtWithReadFence(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	currentLen int,
	ts uint64,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, bool, error) {
	if currentLen >= limit {
		return nil, true, nil
	}

	scanStart := clampScanStart(start, route.Start)
	scanEnd := clampScanEnd(end, route.End)
	kvs, err := s.scanRouteAtDirectionWithReadFence(ctx, route, scanStart, scanEnd, limit-currentLen, ts, true, false, readRouteVersion, routeStart, routeEnd)
	if err != nil {
		return nil, false, err
	}
	return kvs, false, nil
}

func (s *ShardStore) scanRouteAtDirectionWithReadFence(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	explicitGroup bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	if routeScanBoundsPresent(routeStart, routeEnd) {
		return s.scanRouteAtDirectionWithReadFenceRouteFilter(ctx, route, start, end, limit, ts, reverse, explicitGroup, readRouteVersion, routeStart, routeEnd)
	}
	return s.scanRouteAtDirectionWithReadFenceOnce(ctx, route, start, end, limit, ts, reverse, explicitGroup, readRouteVersion, routeStart, routeEnd)
}

func (s *ShardStore) scanRouteAtDirectionWithReadFenceRouteFilter(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	explicitGroup bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	filterStart, filterEnd, empty := routeScanBoundsForRoute(route, routeStart, routeEnd)
	if empty {
		return []*store.KVPair{}, nil
	}
	out := make([]*store.KVPair, 0, min(limit, routeFilteredScanBatchMin))
	scanStart := start
	scanEnd := end
	for len(out) < limit {
		remaining := limit - len(out)
		batchLimit := routeFilteredScanBatchLimit(remaining)
		kvs, cursorKVs, err := s.scanRouteAtDirectionWithReadFenceRouteFilterPage(ctx, route, scanStart, scanEnd, batchLimit, remaining, ts, reverse, explicitGroup, readRouteVersion, filterStart, filterEnd)
		if err != nil {
			return nil, err
		}
		out = appendRouteFilteredKVs(out, kvs, limit, filterStart, filterEnd)
		if routeFilteredScanDone(cursorKVs, batchLimit, len(out), limit) {
			break
		}
		var done bool
		scanStart, scanEnd, done = nextRouteFilteredScanWindow(cursorKVs, scanStart, scanEnd, reverse)
		if done {
			break
		}
	}
	return out, nil
}

func routeScanBoundsForRoute(route distribution.Route, routeStart []byte, routeEnd []byte) ([]byte, []byte, bool) {
	start := routeStart
	if len(route.Start) > 0 && (len(start) == 0 || bytes.Compare(route.Start, start) > 0) {
		start = route.Start
	}
	end := routeEnd
	if len(route.End) > 0 && (len(end) == 0 || bytes.Compare(route.End, end) < 0) {
		end = route.End
	}
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return start, end, true
	}
	return start, end, false
}

func (s *ShardStore) scanRouteAtDirectionWithReadFenceRouteFilterPage(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	visibleLimit int,
	ts uint64,
	reverse bool,
	explicitGroup bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, []*store.KVPair, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil, nil, nil
	}
	markRouteGroup := shouldMarkRouteGroupOnScan(start, explicitGroup, routeStart, routeEnd)

	if engineForGroup(g) == nil {
		kvs, err := s.scanRouteLocal(ctx, g, route, start, end, limit, ts, reverse)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return markScanRouteGroup(filterScanInternalKVs(kvs), route.GroupID, markRouteGroup), markScanRouteGroup(kvs, route.GroupID, markRouteGroup), nil
	}

	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		kvs, cursorKVs, err := s.scanRouteAtLeaderRouteFilter(ctx, g, route, start, end, limit, visibleLimit, ts, reverse, routeStart, routeEnd)
		return markScanRouteGroup(kvs, route.GroupID, markRouteGroup), markScanRouteGroup(cursorKVs, route.GroupID, markRouteGroup), err
	}

	groupID := proxyScanGroupID(route, explicitGroup, readRouteVersion, routeStart, routeEnd)
	kvs, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts, reverse, groupID, readRouteVersion, routeStart, routeEnd)
	if err != nil {
		return nil, nil, err
	}
	filtered := filterScanInternalKVs(kvs)
	return markScanRouteGroup(filtered, route.GroupID, markRouteGroup), markScanRouteGroup(kvs, route.GroupID, markRouteGroup), nil
}

func proxyScanGroupID(route distribution.Route, explicitGroup bool, readRouteVersion uint64, routeStart []byte, routeEnd []byte) uint64 {
	if explicitGroup || readRouteVersion == 0 || routeScanBoundsPresent(routeStart, routeEnd) {
		return route.GroupID
	}
	return 0
}

func (s *ShardStore) scanRouteAtDirectionWithReadFenceOnce(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	explicitGroup bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil, nil
	}
	markRouteGroup := shouldMarkRouteGroupOnScan(start, explicitGroup, routeStart, routeEnd)

	if !reverse {
		return s.scanRouteAtForward(ctx, route, g, start, end, limit, ts, explicitGroup, readRouteVersion, routeStart, routeEnd)
	}

	if engineForGroup(g) == nil {
		kvs, err := s.scanRouteLocal(ctx, g, route, start, end, limit, ts, reverse)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return markScanRouteGroup(filterScanInternalKVs(kvs), route.GroupID, markRouteGroup), nil
	}

	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		kvs, err := s.scanRouteAtLeader(ctx, g, route, start, end, limit, ts, reverse)
		return markScanRouteGroup(kvs, route.GroupID, markRouteGroup), err
	}

	groupID := proxyScanGroupID(route, explicitGroup, readRouteVersion, routeStart, routeEnd)
	kvs, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts, reverse, groupID, readRouteVersion, routeStart, routeEnd)
	if err != nil {
		return nil, err
	}
	// The leader's RawScanAt is expected to perform lock resolution and filtering
	// via ShardStore.ScanAt, so avoid N+1 proxy gets here.
	return markScanRouteGroup(filterScanInternalKVs(kvs), route.GroupID, markRouteGroup), nil
}

const routeFilteredScanBatchMin = 128

func routeFilteredScanBatchLimit(remaining int) int {
	if remaining <= 0 {
		return 0
	}
	limit := remaining
	if limit < routeFilteredScanBatchMin {
		limit = routeFilteredScanBatchMin
	}
	if maxLimit := store.MaxDeltaScanLimit + 1; limit > maxLimit {
		limit = maxLimit
	}
	return limit
}

func routeFilteredScanDone(kvs []*store.KVPair, batchLimit int, outLen int, limit int) bool {
	return len(kvs) == 0 || outLen >= limit || len(kvs) < batchLimit
}

func nextRouteFilteredScanWindow(kvs []*store.KVPair, scanStart []byte, scanEnd []byte, reverse bool) ([]byte, []byte, bool) {
	lastKey := kvs[len(kvs)-1].Key
	if reverse {
		scanEnd = lastKey
		done := len(scanEnd) == 0 || (scanStart != nil && bytes.Compare(scanEnd, scanStart) <= 0)
		return scanStart, scanEnd, done
	}
	scanStart = nextScanCursor(lastKey)
	done := scanEnd != nil && bytes.Compare(scanStart, scanEnd) >= 0
	return scanStart, scanEnd, done
}

func appendRouteFilteredKVs(out []*store.KVPair, kvs []*store.KVPair, limit int, routeStart []byte, routeEnd []byte) []*store.KVPair {
	for _, kvp := range kvs {
		if len(out) >= limit {
			break
		}
		if kvp == nil || !routeKeyInScanBounds(kvp.Key, routeStart, routeEnd) {
			continue
		}
		out = append(out, kvp)
	}
	return out
}

func scanRouteFilteredLockBounds(kvs []*store.KVPair, filteredKVs []*store.KVPair, scanStart []byte, scanEnd []byte, pageLimit int, visibleLimit int, reverse bool) ([]byte, []byte) {
	lockStart, lockEnd := scanLockBoundsForKVsDirection(filteredKVs, scanStart, scanEnd, visibleLimit, reverse)
	pageStart, pageEnd := scanLockBoundsForKVsDirection(kvs, scanStart, scanEnd, pageLimit, reverse)
	return intersectScanBounds(lockStart, lockEnd, pageStart, pageEnd)
}

func intersectScanBounds(aStart []byte, aEnd []byte, bStart []byte, bEnd []byte) ([]byte, []byte) {
	return maxScanStart(aStart, bStart), minScanEnd(aEnd, bEnd)
}

func maxScanStart(a []byte, b []byte) []byte {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if bytes.Compare(a, b) >= 0 {
		return a
	}
	return b
}

func minScanEnd(a []byte, b []byte) []byte {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if bytes.Compare(a, b) <= 0 {
		return a
	}
	return b
}

func routeKeyInScanBounds(key []byte, routeStart []byte, routeEnd []byte) bool {
	if s3BucketAuxiliaryRouteInRange(key, routeStart, routeEnd) {
		return true
	}
	key = routeKey(key)
	if len(routeStart) > 0 && bytes.Compare(key, routeStart) < 0 {
		return false
	}
	if len(routeEnd) > 0 && bytes.Compare(key, routeEnd) >= 0 {
		return false
	}
	return true
}

type scanRoutePage struct {
	kvs        []*store.KVPair
	advanceKey []byte
	full       bool
}

func (s *ShardStore) scanRouteAtForward(
	ctx context.Context,
	route distribution.Route,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	explicitGroup bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	out := make([]*store.KVPair, 0, limit)
	cursor := start
	for len(out) < limit {
		page, err := s.scanRouteAtForwardPage(ctx, route, g, cursor, end, limit, ts, explicitGroup, readRouteVersion, routeStart, routeEnd)
		if err != nil {
			return nil, err
		}
		out = mergeAndTrimScanResultsWithOptions(out, page.kvs, limit, false, false)
		if len(out) >= limit {
			break
		}
		if !page.full || page.advanceKey == nil {
			break
		}
		nextCursor := nextScanCursor(page.advanceKey)
		if end != nil && bytes.Compare(nextCursor, end) >= 0 {
			break
		}
		cursor = nextCursor
	}

	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	if len(out) > limit {
		out = out[:limit]
	}
	markRouteGroup := shouldMarkRouteGroupOnScan(start, explicitGroup, routeStart, routeEnd)
	return markScanRouteGroup(out, route.GroupID, markRouteGroup), nil
}

func (s *ShardStore) scanRouteAtForwardPage(
	ctx context.Context,
	route distribution.Route,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	explicitGroup bool,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) (scanRoutePage, error) {
	engine := engineForGroup(g)
	if engine == nil {
		raw, err := s.scanRouteLocal(ctx, g, route, start, end, limit, ts, false)
		if err != nil {
			return scanRoutePage{}, errors.WithStack(err)
		}
		return scanRoutePage{
			kvs:        filterScanInternalKVs(raw),
			advanceKey: lastKVKey(raw),
			full:       len(raw) >= limit,
		}, nil
	}

	if isLinearizableRaftLeader(ctx, engine) {
		raw, err := s.scanRouteLocal(ctx, g, route, start, end, limit, ts, false)
		if err != nil {
			return scanRoutePage{}, errors.WithStack(err)
		}
		lockStart, lockEnd := scanLockBoundsForKVs(raw, start, end, limit)
		lockKVs, err := scanTxnLockRangeAt(ctx, g, lockStart, lockEnd, ts, limit)
		if err != nil {
			return scanRoutePage{}, err
		}
		kvs, err := s.resolveScanLocks(ctx, g, route, raw, lockKVs, ts)
		if err != nil {
			return scanRoutePage{}, err
		}
		return scanRoutePage{
			kvs:        filterScanInternalKVs(kvs),
			advanceKey: lastKVKey(raw),
			full:       len(raw) >= limit,
		}, nil
	}

	groupID := proxyScanGroupID(route, explicitGroup, readRouteVersion, routeStart, routeEnd)
	raw, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts, false, groupID, readRouteVersion, routeStart, routeEnd)
	if err != nil {
		return scanRoutePage{}, err
	}
	return scanRoutePage{
		kvs:        filterScanInternalKVs(raw),
		advanceKey: lastKVKey(raw),
		full:       len(raw) >= limit,
	}, nil
}

type physicalLimitedStore interface {
	ScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*store.KVPair, bool, error)
	ReverseScanAtPhysicalLimit(ctx context.Context, start []byte, end []byte, visibleLimit, physicalLimit int, ts uint64) ([]*store.KVPair, bool, error)
}

func (s *ShardStore) scanRouteAtDirectionPhysicalLimit(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	visibleLimit int,
	physicalLimit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, bool, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil, false, nil
	}
	markRouteGroup := shouldMarkRouteGroupOnScan(start, false, nil, nil)
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, start, end)
	if err != nil {
		return nil, false, err
	}

	if engineForGroup(g) == nil {
		if routeHasStagedVisibility(route) {
			kvs, err := s.scanRouteLocal(ctx, g, route, start, end, visibleLimit, ts, reverse)
			return markScanRouteGroup(kvs, route.GroupID, markRouteGroup), false, err
		}
		kvs, limitReached, err := scanLocalPhysicalLimit(ctx, g.Store, start, end, visibleLimit, physicalLimit, ts, reverse)
		if err != nil {
			return nil, limitReached, errors.WithStack(err)
		}
		return markScanRouteGroup(filterScanInternalKVs(kvs), route.GroupID, markRouteGroup), limitReached, nil
	}

	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		kvs, limitReached, err := s.scanReadyLeaderPhysicalLimit(ctx, g, route, start, end, visibleLimit, physicalLimit, ts, reverse)
		return markScanRouteGroup(kvs, route.GroupID, markRouteGroup), limitReached, err
	}

	// RawScanAt cannot enforce physicalLimit, so report truncation and let
	// callers fail closed instead of proxying an unbounded physical scan.
	return nil, true, nil
}

func (s *ShardStore) scanReadyLeaderPhysicalLimit(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	visibleLimit int,
	physicalLimit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, bool, error) {
	route, err := s.targetReadyRouteForRange(ctx, g, route, start, end)
	if err != nil {
		return nil, false, err
	}
	if routeHasStagedVisibility(route) {
		kvs, err := s.scanRouteAtLeader(ctx, g, route, start, end, visibleLimit, ts, reverse)
		return kvs, false, err
	}
	return s.scanRouteAtLeaderPhysicalLimit(ctx, g, route, start, end, visibleLimit, physicalLimit, ts, reverse)
}

func scanLocalPhysicalLimit(
	ctx context.Context,
	st store.MVCCStore,
	start []byte,
	end []byte,
	visibleLimit int,
	physicalLimit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, bool, error) {
	scanner, ok := st.(physicalLimitedStore)
	if !ok {
		if reverse {
			kvs, err := st.ReverseScanAt(ctx, start, end, visibleLimit, ts)
			return kvs, false, errors.WithStack(err)
		}
		kvs, err := st.ScanAt(ctx, start, end, visibleLimit, ts)
		return kvs, false, errors.WithStack(err)
	}
	return scanPhysicalLimitLocal(ctx, scanner, start, end, visibleLimit, physicalLimit, ts, reverse)
}

func scanPhysicalLimitLocal(
	ctx context.Context,
	scanner physicalLimitedStore,
	start []byte,
	end []byte,
	visibleLimit int,
	physicalLimit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, bool, error) {
	if reverse {
		kvs, limitReached, err := scanner.ReverseScanAtPhysicalLimit(ctx, start, end, visibleLimit, physicalLimit, ts)
		return kvs, limitReached, errors.WithStack(err)
	}
	kvs, limitReached, err := scanner.ScanAtPhysicalLimit(ctx, start, end, visibleLimit, physicalLimit, ts)
	return kvs, limitReached, errors.WithStack(err)
}

func (s *ShardStore) scanRouteLocal(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, error) {
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, start, end)
	if err != nil {
		return nil, err
	}
	if routeHasStagedVisibility(route) {
		return s.scanRouteWithStagedVisibility(ctx, g, route, start, end, limit, ts, reverse)
	}
	if reverse {
		kvs, err := g.Store.ReverseScanAt(ctx, start, end, limit, ts)
		return kvs, errors.WithStack(err)
	}
	kvs, err := g.Store.ScanAt(ctx, start, end, limit, ts)
	return kvs, errors.WithStack(err)
}

func (s *ShardStore) scanRouteAtLeaderPhysicalLimit(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	visibleLimit int,
	physicalLimit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, bool, error) {
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, start, end)
	if err != nil {
		return nil, false, err
	}
	kvs, limitReached, err := scanLocalPhysicalLimit(ctx, g.Store, start, end, visibleLimit, physicalLimit, ts, reverse)
	if err != nil {
		return nil, limitReached, errors.WithStack(err)
	}
	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, start, end, visibleLimit, reverse)
	lockKVs, err := scanTxnLockRangeAt(ctx, g, lockStart, lockEnd, ts, visibleLimit)
	if err != nil {
		return nil, limitReached, err
	}
	resolved, err := s.resolveScanLocks(ctx, g, route, kvs, lockKVs, ts)
	return resolved, limitReached, err
}

func (s *ShardStore) scanRouteAtLeader(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, error) {
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, start, end)
	if err != nil {
		return nil, err
	}
	var kvs []*store.KVPair
	switch {
	case routeHasStagedVisibility(route):
		kvs, err = s.scanRouteWithStagedVisibility(ctx, g, route, start, end, limit, ts, reverse)
	case reverse:
		kvs, err = g.Store.ReverseScanAt(ctx, start, end, limit, ts)
	default:
		kvs, err = g.Store.ScanAt(ctx, start, end, limit, ts)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, start, end, limit, reverse)
	lockKVs, err := scanTxnLockRangeAt(ctx, g, lockStart, lockEnd, ts, limit)
	if err != nil {
		return nil, err
	}
	return s.resolveScanLocks(ctx, g, route, kvs, lockKVs, ts)
}

const (
	stagedVisibilityMaxCandidateWindow = 8192
	stagedVisibilityWindowGrowthFactor = 2
)

func (s *ShardStore) scanRouteWithStagedVisibility(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, error) {
	if err := ensureReadTSRetained(g.Store, ts); err != nil {
		return nil, err
	}
	out := make([]*store.KVPair, 0, limit)
	scanStart := bytes.Clone(start)
	scanEnd := bytes.Clone(end)
	for len(out) < limit {
		remaining := limit - len(out)
		kvs, boundary, hasMore, err := s.scanRouteWithStagedVisibilityPage(ctx, g, route, scanStart, scanEnd, remaining, ts, reverse)
		if err != nil {
			return nil, err
		}
		out = append(out, kvs...)
		if len(out) >= limit {
			clear(out[limit:])
			return out[:limit], nil
		}
		if !hasMore {
			return out, nil
		}
		if reverse {
			scanEnd = boundary
		} else {
			scanStart = exclusiveScanStartAfter(boundary)
		}
	}
	return out, nil
}

func (s *ShardStore) scanRouteWithStagedVisibilityPage(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, []byte, bool, error) {
	stagedStart, stagedEnd := stagedVisibilityScanBounds(route.MigrationJobID, start, end)
	window := stagedVisibilityCandidateWindow(limit)
	for {
		liveKVs, err := scanVisibleCandidates(ctx, g.Store, start, end, window, ts, reverse)
		if err != nil {
			return nil, nil, false, err
		}
		stagedKVs, err := scanVisibleCandidates(ctx, g.Store, stagedStart, stagedEnd, window, ts, reverse)
		if err != nil {
			return nil, nil, false, err
		}
		versions, err := s.latestStagedVisibilityCandidates(ctx, g.Store, route, liveKVs, stagedKVs, ts)
		if err != nil {
			return nil, nil, false, err
		}
		out := visibleLogicalKVs(versions, ts, reverse)
		out = filterRouteScanKVs(out, route.Start, route.End)
		liveExhausted := len(liveKVs) < window
		stagedExhausted := len(stagedKVs) < window
		boundary, hasBoundary := stagedVisibilityCandidateBoundary(liveKVs, stagedKVs, liveExhausted, stagedExhausted, reverse)
		exhausted := liveExhausted && stagedExhausted
		out = stagedVisibilityKVsWithinPageBoundary(out, boundary, hasBoundary, exhausted, reverse)
		if len(out) >= limit {
			clear(out[limit:])
			return out[:limit], boundary, !exhausted && hasBoundary, nil
		}
		if exhausted {
			return out, nil, false, nil
		}
		nextWindow := nextStagedVisibilityCandidateWindow(window)
		if nextWindow == window {
			return out, boundary, hasBoundary, nil
		}
		window = nextWindow
	}
}

func stagedVisibilityKVsWithinPageBoundary(kvs []*store.KVPair, boundary []byte, hasBoundary bool, exhausted bool, reverse bool) []*store.KVPair {
	if exhausted || !hasBoundary {
		return kvs
	}
	return stagedVisibilityKVsWithinBoundary(kvs, boundary, reverse)
}

func stagedVisibilityKVsWithinBoundary(kvs []*store.KVPair, boundary []byte, reverse bool) []*store.KVPair {
	if len(boundary) == 0 {
		return kvs
	}
	n := 0
	for _, kvp := range kvs {
		if kvp == nil {
			continue
		}
		cmp := bytes.Compare(kvp.Key, boundary)
		if (!reverse && cmp <= 0) || (reverse && cmp >= 0) {
			kvs[n] = kvp
			n++
		}
	}
	clear(kvs[n:])
	return kvs[:n]
}

func stagedVisibilityCandidateBoundary(liveKVs []*store.KVPair, stagedKVs []*store.KVPair, liveExhausted bool, stagedExhausted bool, reverse bool) ([]byte, bool) {
	liveBoundary := stagedVisibilityBoundary{reverse: reverse}
	for _, kvp := range liveKVs {
		if kvp == nil {
			continue
		}
		if isMigrationStagedDataKey(kvp.Key) {
			continue
		}
		liveBoundary.visit(kvp.Key)
	}
	stagedBoundary := stagedVisibilityBoundary{reverse: reverse}
	for _, kvp := range stagedKVs {
		rawKey, stagedOK := stagedVisibilityRawCandidateKey(kvp)
		if !stagedOK {
			continue
		}
		stagedBoundary.visit(rawKey)
	}
	return mergeStagedVisibilityBoundaries(liveBoundary, stagedBoundary, liveExhausted, stagedExhausted, reverse)
}

func mergeStagedVisibilityBoundaries(live stagedVisibilityBoundary, staged stagedVisibilityBoundary, liveExhausted bool, stagedExhausted bool, reverse bool) ([]byte, bool) {
	if !live.ok {
		return staged.key, staged.ok
	}
	if !staged.ok {
		return live.key, live.ok
	}
	if !liveExhausted && !stagedExhausted {
		return nearerStagedVisibilityBoundary(live.key, staged.key, reverse), true
	}
	if liveExhausted && stagedExhausted {
		return fartherStagedVisibilityBoundary(live.key, staged.key, reverse), true
	}
	if liveExhausted {
		return staged.key, true
	}
	return live.key, true
}

func nearerStagedVisibilityBoundary(a []byte, b []byte, reverse bool) []byte {
	cmp := bytes.Compare(a, b)
	if (!reverse && cmp <= 0) || (reverse && cmp >= 0) {
		return a
	}
	return b
}

func fartherStagedVisibilityBoundary(a []byte, b []byte, reverse bool) []byte {
	cmp := bytes.Compare(a, b)
	if (!reverse && cmp >= 0) || (reverse && cmp <= 0) {
		return a
	}
	return b
}

type stagedVisibilityBoundary struct {
	key     []byte
	ok      bool
	reverse bool
}

func (b *stagedVisibilityBoundary) visit(key []byte) {
	if !b.ok {
		b.key = bytes.Clone(key)
		b.ok = true
		return
	}
	cmp := bytes.Compare(key, b.key)
	if (!b.reverse && cmp > 0) || (b.reverse && cmp < 0) {
		b.key = bytes.Clone(key)
	}
}

func stagedVisibilityRawCandidateKey(kvp *store.KVPair) ([]byte, bool) {
	if kvp == nil {
		return nil, false
	}
	_, rawKey, ok := distribution.MigrationStagedDataKeyParts(kvp.Key)
	return rawKey, ok
}

func exclusiveScanStartAfter(key []byte) []byte {
	if key == nil {
		return nil
	}
	out := bytes.Clone(key)
	return append(out, 0)
}

func stagedVisibilityCandidateWindow(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > stagedVisibilityMaxCandidateWindow {
		return stagedVisibilityMaxCandidateWindow
	}
	return limit
}

func nextStagedVisibilityCandidateWindow(window int) int {
	if window >= stagedVisibilityMaxCandidateWindow {
		return window
	}
	next := window * stagedVisibilityWindowGrowthFactor
	if next < window || next > stagedVisibilityMaxCandidateWindow {
		return stagedVisibilityMaxCandidateWindow
	}
	return next
}

func scanVisibleCandidates(ctx context.Context, st store.MVCCStore, start, end []byte, limit int, ts uint64, reverse bool) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	if reverse {
		kvs, err := st.ReverseScanAt(ctx, start, end, limit, ts)
		return kvs, errors.WithStack(err)
	}
	kvs, err := st.ScanAt(ctx, start, end, limit, ts)
	return kvs, errors.WithStack(err)
}

func (s *ShardStore) latestStagedVisibilityCandidates(
	ctx context.Context,
	st store.MVCCStore,
	route distribution.Route,
	liveKVs []*store.KVPair,
	stagedKVs []*store.KVPair,
	ts uint64,
) (map[string]store.MVCCVersion, error) {
	keys := stagedVisibilityCandidateKeys(liveKVs, stagedKVs)
	out := make(map[string]store.MVCCVersion, len(keys))
	for _, key := range keys {
		live, liveOK, err := latestMVCCVersionAt(ctx, st, key, ts)
		if err != nil {
			return nil, err
		}
		stagedKey := distribution.MigrationStagedDataKey(route.MigrationJobID, key)
		staged, stagedOK, err := latestMVCCVersionAt(ctx, st, stagedKey, ts)
		if err != nil {
			return nil, err
		}
		if stagedOK {
			staged.Key = bytes.Clone(key)
		}
		if winner, ok := newerMigrationVersion(live, liveOK, staged, stagedOK); ok {
			out[string(key)] = winner
		}
	}
	return out, nil
}

func stagedVisibilityCandidateKeys(liveKVs []*store.KVPair, stagedKVs []*store.KVPair) [][]byte {
	seen := make(map[string][]byte, len(liveKVs)+len(stagedKVs))
	for _, kvp := range liveKVs {
		if kvp == nil {
			continue
		}
		if isMigrationStagedDataKey(kvp.Key) {
			continue
		}
		seen[string(kvp.Key)] = bytes.Clone(kvp.Key)
	}
	for _, kvp := range stagedKVs {
		if kvp == nil {
			continue
		}
		_, rawKey, ok := distribution.MigrationStagedDataKeyParts(kvp.Key)
		if !ok {
			continue
		}
		seen[string(rawKey)] = bytes.Clone(rawKey)
	}
	out := make([][]byte, 0, len(seen))
	for _, key := range seen {
		out = append(out, key)
	}
	return out
}

func isMigrationStagedDataKey(key []byte) bool {
	_, _, ok := distribution.MigrationStagedDataKeyParts(key)
	return ok
}

func stagedVisibilityScanBounds(jobID uint64, start []byte, end []byte) ([]byte, []byte) {
	prefix := distribution.MigrationStagedDataKeyPrefix(jobID)
	scanStart := prefix
	if start != nil {
		scanStart = distribution.MigrationStagedDataKey(jobID, start)
	}
	scanEnd := prefixScanEnd(prefix)
	if end != nil {
		scanEnd = distribution.MigrationStagedDataKey(jobID, end)
	}
	return scanStart, scanEnd
}

func visibleLogicalKVs(versions map[string]store.MVCCVersion, ts uint64, reverse bool) []*store.KVPair {
	out := make([]*store.KVPair, 0, len(versions))
	for _, version := range versions {
		if !migrationVersionVisible(version, ts) {
			continue
		}
		out = append(out, &store.KVPair{
			Key:   bytes.Clone(version.Key),
			Value: bytes.Clone(version.Value),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		cmp := bytes.Compare(out[i].Key, out[j].Key)
		if reverse {
			return cmp > 0
		}
		return cmp < 0
	})
	return out
}

func (s *ShardStore) scanRouteAtLeaderRouteFilter(
	ctx context.Context,
	g *ShardGroup,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	visibleLimit int,
	ts uint64,
	reverse bool,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, []*store.KVPair, error) {
	var (
		kvs []*store.KVPair
		err error
	)
	switch {
	case routeHasStagedVisibility(route):
		kvs, err = s.scanRouteWithStagedVisibility(ctx, g, route, start, end, limit, ts, reverse)
	case reverse:
		kvs, err = g.Store.ReverseScanAt(ctx, start, end, limit, ts)
	default:
		kvs, err = g.Store.ScanAt(ctx, start, end, limit, ts)
	}
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	filteredKVs := filterRouteScanKVs(kvs, routeStart, routeEnd)
	lockStart, lockEnd := scanRouteFilteredLockBounds(kvs, filteredKVs, start, end, limit, visibleLimit, reverse)
	lockKVs, err := scanTxnLockRangeAtWithRouteFilter(ctx, g, lockStart, lockEnd, ts, visibleLimit, routeStart, routeEnd)
	if err != nil {
		return nil, nil, err
	}
	resolved, err := s.resolveScanLocks(ctx, g, route, filteredKVs, lockKVs, ts)
	return resolved, kvs, err
}

func filterRouteScanKVs(kvs []*store.KVPair, routeStart []byte, routeEnd []byte) []*store.KVPair {
	if len(kvs) == 0 {
		return kvs
	}
	out := make([]*store.KVPair, 0, len(kvs))
	for _, kvp := range kvs {
		if kvp == nil || !routeKeyInScanBounds(kvp.Key, routeStart, routeEnd) {
			continue
		}
		out = append(out, kvp)
	}
	return out
}

func scanLockBoundsForKVs(kvs []*store.KVPair, scanStart []byte, scanEnd []byte, limit int) ([]byte, []byte) {
	return scanLockBoundsForKVsDirection(kvs, scanStart, scanEnd, limit, false)
}

func scanLockBoundsForKVsDirection(kvs []*store.KVPair, scanStart []byte, scanEnd []byte, limit int, reverse bool) ([]byte, []byte) {
	if len(kvs) < limit {
		return scanStart, scanEnd
	}
	firstUserKey, lastUserKey, ok := observedScanUserBounds(kvs)
	if ok {
		if reverse {
			if len(scanStart) == 0 || bytes.Compare(firstUserKey, scanStart) > 0 {
				scanStart = firstUserKey
			}
			return scanStart, scanEnd
		}
		return scanStart, boundScanEnd(scanEnd, nextScanCursor(lastUserKey))
	}
	if reverse {
		return scanStart, scanEnd
	}
	if lastKey := lastKVKey(kvs); lastKey != nil {
		return scanStart, boundScanEnd(scanEnd, nextScanCursor(lastKey))
	}
	return scanStart, scanEnd
}

func boundScanEnd(scanEnd []byte, bound []byte) []byte {
	if scanEnd == nil || bytes.Compare(bound, scanEnd) < 0 {
		return bound
	}
	return scanEnd
}

func observedScanUserBounds(kvs []*store.KVPair) ([]byte, []byte, bool) {
	var minKey []byte
	var maxKey []byte
	seen := false
	for _, kvp := range kvs {
		userKey, ok := scanUserKey(kvp)
		if !ok {
			continue
		}
		if !seen || bytes.Compare(userKey, minKey) < 0 {
			minKey = userKey
		}
		if !seen || bytes.Compare(userKey, maxKey) > 0 {
			maxKey = userKey
		}
		seen = true
	}
	if !seen {
		return nil, nil, false
	}
	return minKey, maxKey, true
}

func scanUserKey(kvp *store.KVPair) ([]byte, bool) {
	if kvp == nil || kvp.Key == nil {
		return nil, false
	}
	if isMigrationStagedDataKey(kvp.Key) {
		return nil, false
	}
	if !isTxnInternalKey(kvp.Key) {
		return kvp.Key, true
	}
	return txnUserKeyFromLockKey(kvp.Key)
}

func mergeAndTrimScanResultsWithOptions(out []*store.KVPair, kvs []*store.KVPair, limit int, reverse bool, dedupeByKey bool) []*store.KVPair {
	if len(kvs) == 0 {
		return out
	}
	if dedupeByKey {
		out = appendReplacingKVsByKey(out, kvs)
	} else {
		out = append(out, kvs...)
	}
	sort.Slice(out, func(i, j int) bool {
		cmp := bytes.Compare(out[i].Key, out[j].Key)
		if reverse {
			return cmp > 0
		}
		return cmp < 0
	})
	if len(out) <= limit {
		return out
	}
	clear(out[limit:])
	return out[:limit]
}

func mergeAndTrimScanKeys(out [][]byte, keys [][]byte, limit int) [][]byte {
	if len(keys) == 0 {
		return out
	}
	out = append(out, keys...)
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i], out[j]) < 0
	})
	write := 0
	for _, key := range out {
		if key == nil {
			continue
		}
		if write > 0 && bytes.Equal(out[write-1], key) {
			continue
		}
		out[write] = key
		write++
	}
	clear(out[write:])
	out = out[:write]
	if len(out) <= limit {
		return out
	}
	clear(out[limit:])
	return out[:limit]
}

func mergeAndTrimReverseScanResults(out []*store.KVPair, kvs []*store.KVPair, limit int) []*store.KVPair {
	return mergeAndTrimScanResultsWithOptions(out, kvs, limit, true, false)
}

func appendReplacingKVsByKey(out []*store.KVPair, kvs []*store.KVPair) []*store.KVPair {
	indexByKey := make(map[string]int, len(out)+len(kvs))
	for i, kvp := range out {
		if kvp == nil {
			continue
		}
		indexByKey[string(kvp.Key)] = i
	}
	for _, kvp := range kvs {
		if kvp == nil {
			continue
		}
		key := string(kvp.Key)
		if idx, ok := indexByKey[key]; ok {
			out[idx] = kvp
			continue
		}
		indexByKey[key] = len(out)
		out = append(out, kvp)
	}
	return out
}

func kvPairsFromKeys(keys [][]byte) []*store.KVPair {
	kvs := make([]*store.KVPair, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			continue
		}
		kvs = append(kvs, &store.KVPair{Key: key})
	}
	return kvs
}

func keysFromKVs(kvs []*store.KVPair) [][]byte {
	keys := make([][]byte, 0, len(kvs))
	for _, kvp := range kvs {
		if kvp == nil || kvp.Key == nil {
			continue
		}
		keys = append(keys, kvp.Key)
	}
	return keys
}

func lastKVKey(kvs []*store.KVPair) []byte {
	for i := len(kvs) - 1; i >= 0; i-- {
		if kvs[i] == nil || kvs[i].Key == nil {
			continue
		}
		return kvs[i].Key
	}
	return nil
}

func filterTxnInternalKeys(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return keys
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if key == nil || isTxnInternalKey(key) {
			continue
		}
		out = append(out, key)
	}
	return out
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
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g == nil || g.Store == nil {
		return store.ErrNotSupported
	}
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return err
	}
	if err := ensureRouteWriteTimestampFloor(route, key, commitTS); err != nil {
		return err
	}
	if err := s.ensureS3BucketAuxiliaryWriteTimestampFloor(key, commitTS); err != nil {
		return err
	}
	return errors.WithStack(g.Store.PutAt(ctx, key, value, commitTS, expireAt))
}

func (s *ShardStore) DeleteAt(ctx context.Context, key []byte, commitTS uint64) error {
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g == nil || g.Store == nil {
		return store.ErrNotSupported
	}
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return err
	}
	if err := ensureRouteWriteTimestampFloor(route, key, commitTS); err != nil {
		return err
	}
	if err := s.ensureS3BucketAuxiliaryWriteTimestampFloor(key, commitTS); err != nil {
		return err
	}
	return errors.WithStack(g.Store.DeleteAt(ctx, key, commitTS))
}

func (s *ShardStore) PutWithTTLAt(ctx context.Context, key []byte, value []byte, commitTS uint64, expireAt uint64) error {
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g == nil || g.Store == nil {
		return store.ErrNotSupported
	}
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return err
	}
	if err := ensureRouteWriteTimestampFloor(route, key, commitTS); err != nil {
		return err
	}
	if err := s.ensureS3BucketAuxiliaryWriteTimestampFloor(key, commitTS); err != nil {
		return err
	}
	return errors.WithStack(g.Store.PutWithTTLAt(ctx, key, value, commitTS, expireAt))
}

func (s *ShardStore) ExpireAt(ctx context.Context, key []byte, expireAt uint64, commitTS uint64) error {
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g == nil || g.Store == nil {
		return store.ErrNotSupported
	}
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return err
	}
	if err := ensureRouteWriteTimestampFloor(route, key, commitTS); err != nil {
		return err
	}
	if err := s.ensureS3BucketAuxiliaryWriteTimestampFloor(key, commitTS); err != nil {
		return err
	}
	return errors.WithStack(g.Store.ExpireAt(ctx, key, expireAt, commitTS))
}

func (s *ShardStore) LatestCommitTS(ctx context.Context, key []byte) (uint64, bool, error) {
	return s.LatestCommitTSWithReadFence(ctx, key, 0)
}

func (s *ShardStore) LatestCommitTSWithReadFence(ctx context.Context, key []byte, readRouteVersion uint64) (uint64, bool, error) {
	if err := s.awaitReadRouteVersion(ctx, readRouteVersion); err != nil {
		return 0, false, err
	}
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g.Store == nil {
		return 0, false, nil
	}

	if engineForGroup(g) == nil {
		ts, exists, err := s.localLatestCommitTS(ctx, g, route, key)
		if err != nil {
			return 0, false, errors.WithStack(err)
		}
		return ts, exists, nil
	}

	// Avoid returning a stale watermark when our local raft instance is a
	// deposed leader. Lease-aware: on lease hit we skip the read-index
	// round-trip (same rationale as isLinearizableRaftLeader).
	if engine := engineForGroup(g); isLeaderEngine(engine) {
		if _, err := leaseReadEngineCtx(ctx, engine); err == nil {
			ts, exists, err := s.localLatestCommitTS(ctx, g, route, key)
			if err != nil {
				return 0, false, errors.WithStack(err)
			}
			return ts, exists, nil
		}
	}

	return s.proxyLatestCommitTS(ctx, g, key, readRouteVersion)
}

func (s *ShardStore) localLatestCommitTS(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte) (uint64, bool, error) {
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return 0, false, err
	}
	liveTS, liveExists, err := g.Store.LatestCommitTS(ctx, key)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	if !routeHasStagedVisibility(route) {
		return liveTS, liveExists, nil
	}
	stagedTS, stagedExists, err := g.Store.LatestCommitTS(ctx, distribution.MigrationStagedDataKey(route.MigrationJobID, key))
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	switch {
	case !liveExists:
		return stagedTS, stagedExists, nil
	case !stagedExists:
		return liveTS, true, nil
	case stagedTS >= liveTS:
		return stagedTS, true, nil
	default:
		return liveTS, true, nil
	}
}

func (s *ShardStore) proxyLatestCommitTS(ctx context.Context, g *ShardGroup, key []byte, readRouteVersion uint64) (uint64, bool, error) {
	engine := engineForGroup(g)
	if engine == nil {
		return 0, false, nil
	}
	addr := leaderAddrFromEngine(engine)
	if addr == "" {
		return 0, false, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return 0, false, err
	}

	ctx, cancel := context.WithTimeout(ctx, proxyForwardTimeout)
	defer cancel()
	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: key, ReadRouteVersion: readRouteVersion})
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
		return applyTxnResolution(ctx, g, pb.Phase_COMMIT, lock.StartTS, commitTS, lock.PrimaryKey, [][]byte{key})
	case txnStatusRolledBack:
		abortTS := abortTSFrom(lock.StartTS, commitTS)
		if abortTS <= lock.StartTS {
			// Defensive check: While uint64 overflow is not expected in normal operation,
			// this handles the edge case where startTS==^uint64(0) or a bug causes overflow.
			// Prevents violating the FSM invariant resolveTS > startTS (fsm.go:258).
			return NewTxnLockedErrorWithDetail(key, "timestamp overflow")
		}
		return applyTxnResolution(ctx, g, pb.Phase_ABORT, lock.StartTS, abortTS, lock.PrimaryKey, [][]byte{key})
	case txnStatusPending:
		return NewTxnLockedError(key)
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
	cleanupNow        uint64
}

func newScanLockPlan(size int) *scanLockPlan {
	return &scanLockPlan{
		items:             make([]scanItem, 0, size),
		itemIndex:         make(map[string]int, size),
		statusCache:       make(map[lockTxnKey]lockTxnStatus),
		resolutionBatches: make(map[lockTxnKey]*lockResolutionBatch),
		batchOrder:        make([]lockTxnKey, 0),
		cleanupNow:        hlcWallNow(),
	}
}

func (s *ShardStore) resolveScanLocks(ctx context.Context, g *ShardGroup, route distribution.Route, kvs []*store.KVPair, lockKVs []*store.KVPair, ts uint64) ([]*store.KVPair, error) {
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
	if err := applyScanLockResolutions(ctx, g, plan); err != nil {
		return nil, err
	}
	return s.materializeScanLockResults(ctx, g, route, ts, plan.items)
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
	if kvp == nil || isScanInternalKey(kvp.Key) {
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
	phase, resolveTS, err := lockResolutionForStatus(state, lock, userKey, plan.cleanupNow)
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

func lockResolutionForStatus(state lockTxnStatus, lock txnLock, key []byte, cleanupNow uint64) (pb.Phase, uint64, error) {
	switch state.status {
	case txnStatusPending:
		return pb.Phase_NONE, 0, NewTxnLockedError(key)
	case txnStatusCommitted:
		return pb.Phase_COMMIT, state.commitTS, nil
	case txnStatusRolledBack:
		abortTS := cleanupTSWithNow(lock.StartTS, cleanupNow)
		if abortTS <= lock.StartTS {
			return pb.Phase_NONE, 0, NewTxnLockedErrorWithDetail(key, "timestamp overflow")
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
	if kvp == nil || kvp.Key == nil {
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

func scanTxnLockRangeAt(ctx context.Context, g *ShardGroup, start []byte, end []byte, ts uint64, limit int) ([]*store.KVPair, error) {
	if g == nil || g.Store == nil {
		return []*store.KVPair{}, nil
	}

	lockStart, lockEnd := txnLockScanBounds(start, end)
	return scanTxnLockPagesAt(ctx, g.Store, lockStart, lockEnd, ts, boundedTxnLockScanLimit(limit))
}

func scanTxnLockRangeAtWithRouteFilter(ctx context.Context, g *ShardGroup, start []byte, end []byte, ts uint64, limit int, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	if g == nil || g.Store == nil {
		return []*store.KVPair{}, nil
	}
	if !routeScanBoundsPresent(routeStart, routeEnd) {
		return scanTxnLockRangeAt(ctx, g, start, end, ts, limit)
	}

	lockStart, lockEnd := txnLockScanBounds(start, end)
	return scanTxnLockPagesAtWithRouteFilter(ctx, g.Store, lockStart, lockEnd, ts, boundedTxnLockScanLimit(limit), routeStart, routeEnd)
}

func scanTxnLockPagesAt(ctx context.Context, st store.MVCCStore, start []byte, end []byte, ts uint64, limit int) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0, min(limit, lockPageLimit))
	cursor := start
	for {
		lockKVs, nextCursor, done, err := scanTxnLockPageAt(ctx, st, cursor, end, ts)
		if err != nil {
			return nil, err
		}
		out = append(out, lockKVs...)
		if len(out) >= limit && !done {
			return nil, errors.Wrapf(ErrTxnLocked, "scan lock budget exceeded for range [%q,%q)", string(start), string(end))
		}
		if done {
			if len(out) > limit {
				out = out[:limit]
			}
			return out, nil
		}
		cursor = nextCursor
	}
}

func scanTxnLockPagesAtWithRouteFilter(ctx context.Context, st store.MVCCStore, start []byte, end []byte, ts uint64, limit int, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0, min(limit, lockPageLimit))
	cursor := start
	for {
		lockKVs, nextCursor, done, err := scanTxnLockPageAt(ctx, st, cursor, end, ts)
		if err != nil {
			return nil, err
		}
		for _, kvp := range lockKVs {
			if kvp == nil || !routeKeyInScanBounds(kvp.Key, routeStart, routeEnd) {
				continue
			}
			out = append(out, kvp)
			if len(out) > limit {
				return nil, errors.Wrapf(ErrTxnLocked, "scan lock budget exceeded for range [%q,%q)", string(start), string(end))
			}
		}
		if done {
			return out, nil
		}
		cursor = nextCursor
	}
}

const lockPageLimit = 256
const maxTxnLockScanResults = 1024

func boundedTxnLockScanLimit(limit int) int {
	if limit < lockPageLimit {
		return lockPageLimit
	}
	if limit > maxTxnLockScanResults {
		return maxTxnLockScanResults
	}
	return limit
}

func scanTxnLockPageAt(ctx context.Context, st store.MVCCStore, start []byte, end []byte, ts uint64) ([]*store.KVPair, []byte, bool, error) {
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

func applyScanLockResolutions(ctx context.Context, g *ShardGroup, plan *scanLockPlan) error {
	for _, txnKey := range plan.batchOrder {
		batch := plan.resolutionBatches[txnKey]
		if err := applyTxnResolution(ctx, g, batch.phase, batch.startTS, batch.resolveTS, batch.primaryKey, batch.keys); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) materializeScanLockResults(ctx context.Context, g *ShardGroup, route distribution.Route, ts uint64, items []scanItem) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0, len(items))
	for _, item := range items {
		if item.skip {
			continue
		}
		if !item.locked {
			out = append(out, item.kvp)
			continue
		}
		v, err := s.localGetAt(ctx, g, route, item.kvp.Key, ts)
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

func filterScanInternalKVs(kvs []*store.KVPair) []*store.KVPair {
	if len(kvs) == 0 {
		return kvs
	}
	out := make([]*store.KVPair, 0, len(kvs))
	for _, kvp := range kvs {
		if kvp == nil {
			continue
		}
		if isScanInternalKey(kvp.Key) {
			continue
		}
		out = append(out, kvp)
	}
	return out
}

func isScanInternalKey(key []byte) bool {
	return isTxnInternalKey(key) || isMigrationStagedDataKey(key)
}

func markScanRouteGroup(kvs []*store.KVPair, groupID uint64, mark bool) []*store.KVPair {
	if !mark || groupID == 0 {
		return kvs
	}
	for _, kvp := range kvs {
		if kvp != nil {
			kvp.RouteGroupID = groupID
		}
	}
	return kvs
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
	aborted, err := s.tryAbortExpiredPrimary(ctx, primaryKey, startTS)
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

func (s *ShardStore) tryAbortExpiredPrimary(ctx context.Context, primaryKey []byte, startTS uint64) (bool, error) {
	pg, ok := s.groupForKey(primaryKey)
	if !ok || pg == nil || pg.Txn == nil {
		return false, nil
	}
	// No commitTS available here; we're aborting an expired lock with no commit record.
	// Pass 0 for commitTS to explicitly indicate it's not available; abortTSFrom will
	// use startTS+1 if representable.
	abortTS := abortTSFrom(startTS, 0)
	if abortTS <= startTS {
		// Defensive check: While uint64 overflow is not expected in normal operation,
		// this handles the edge case where startTS==^uint64(0) or a bug causes overflow.
		// Prevents violating the FSM invariant resolveTS > startTS (fsm.go:258).
		return false, nil
	}
	if err := applyTxnResolution(ctx, pg, pb.Phase_ABORT, startTS, abortTS, primaryKey, [][]byte{primaryKey}); err != nil {
		return false, err
	}
	return true, nil
}

func applyTxnResolution(ctx context.Context, g *ShardGroup, phase pb.Phase, startTS, commitTS uint64, primaryKey []byte, keys [][]byte) error {
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
	_, err := g.Txn.Commit(ctx, []*pb.Request{{IsTxn: true, Phase: phase, Ts: startTS, Mutations: muts}})
	return errors.WithStack(err)
}

func cleanupTSWithNow(startTS, now uint64) uint64 {
	next := startTS + 1
	if now > next {
		return now
	}
	return next
}

// ApplyMutations applies a batch of mutations to the correct shard store.
//
// All mutations must belong to the same shard. Cross-shard mutation batches are
// not supported.
func (s *ShardStore) ApplyMutations(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	group, err := s.resolveSingleShardGroup(mutations)
	if err != nil || group == nil {
		return err
	}
	if err := s.verifyMutationRoutes(ctx, mutations, readKeys, commitTS); err != nil {
		return err
	}
	readKeys = s.readKeysWithStagedVisibilityAliases(group, readKeys)
	readKeys = s.readKeysWithStagedVisibilityMutationAliases(group, readKeys, mutations)
	return errors.WithStack(group.Store.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS))
}

func (s *ShardStore) verifyMutationRoutes(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, commitTS uint64) error {
	for _, mut := range mutations {
		if err := s.verifyMutationWriteRoute(ctx, mut.Key, commitTS); err != nil {
			return err
		}
	}
	for _, key := range readKeys {
		route, g, ok := s.routeAndGroupForKey(key)
		if !ok || g == nil || g.Store == nil {
			return store.ErrNotSupported
		}
		if err := s.verifyTargetReadinessForRange(ctx, g, route, key, nextScanCursor(key)); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) verifyMutationWriteRoute(ctx context.Context, key []byte, commitTS uint64) error {
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g == nil || g.Store == nil {
		return store.ErrNotSupported
	}
	var err error
	route, err = s.targetReadyRouteForRange(ctx, g, route, key, nextScanCursor(key))
	if err != nil {
		return err
	}
	if err := ensureRouteWriteTimestampFloor(route, key, commitTS); err != nil {
		return err
	}
	return s.ensureS3BucketAuxiliaryWriteTimestampFloor(key, commitTS)
}

// ApplyMutationsRaft is the raft-apply variant; see store.MVCCStore for the
// durability contract. Only the FSM may call this method.
func (s *ShardStore) ApplyMutationsRaft(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	group, err := s.resolveSingleShardGroup(mutations)
	if err != nil || group == nil {
		return err
	}
	readKeys = s.readKeysWithStagedVisibilityAliases(group, readKeys)
	readKeys = s.readKeysWithStagedVisibilityMutationAliases(group, readKeys, mutations)
	return errors.WithStack(group.Store.ApplyMutationsRaft(ctx, mutations, readKeys, startTS, commitTS))
}

// ApplyMutationsRaftAt is the raft-entry-index-aware variant. Threads
// appliedIndex through to the single owning shard so the leaf can
// bundle metaAppliedIndex with the mutation. See PR #910 design §2.
func (s *ShardStore) ApplyMutationsRaftAt(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS, appliedIndex uint64) error {
	group, err := s.resolveSingleShardGroup(mutations)
	if err != nil || group == nil {
		return err
	}
	readKeys = s.readKeysWithStagedVisibilityAliases(group, readKeys)
	readKeys = s.readKeysWithStagedVisibilityMutationAliases(group, readKeys, mutations)
	return errors.WithStack(group.Store.ApplyMutationsRaftAt(ctx, mutations, readKeys, startTS, commitTS, appliedIndex))
}

func ensureRouteWriteTimestampFloor(route distribution.Route, key []byte, commitTS uint64) error {
	if route.MinWriteTSExclusive == 0 || commitTS == 0 || commitTS > route.MinWriteTSExclusive {
		return nil
	}
	return errors.Wrapf(ErrRouteWriteTimestampTooLow, "key %q routeKey %q commit_ts=%d floor=%d", key, routeKey(key), commitTS, route.MinWriteTSExclusive)
}

func (s *ShardStore) ensureS3BucketAuxiliaryWriteTimestampFloor(key []byte, commitTS uint64) error {
	if s == nil || s.engine == nil || commitTS == 0 {
		return nil
	}
	start, end, ok := s3BucketAuxiliaryRouteRange(key)
	if !ok {
		return nil
	}
	for _, route := range s.engine.GetIntersectingRoutes(start, end) {
		if route.MinWriteTSExclusive != 0 && commitTS <= route.MinWriteTSExclusive {
			return errors.Wrapf(ErrRouteWriteTimestampTooLow, "key %q route range [%q,%q) commit_ts=%d floor=%d", key, start, end, commitTS, route.MinWriteTSExclusive)
		}
	}
	return nil
}

func (s *ShardStore) readKeysWithStagedVisibilityAliases(group *ShardGroup, readKeys [][]byte) [][]byte {
	if len(readKeys) == 0 {
		return readKeys
	}
	for _, key := range readKeys {
		readKeys = s.appendStagedVisibilityAlias(group, readKeys, key)
	}
	return readKeys
}

func (s *ShardStore) readKeysWithStagedVisibilityMutationAliases(group *ShardGroup, readKeys [][]byte, mutations []*store.KVPairMutation) [][]byte {
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		readKeys = s.appendStagedVisibilityAlias(group, readKeys, mut.Key)
	}
	return readKeys
}

func (s *ShardStore) appendStagedVisibilityAlias(group *ShardGroup, readKeys [][]byte, key []byte) [][]byte {
	if s == nil || s.engine == nil || group == nil {
		return readKeys
	}
	alias, ok := s.stagedVisibilityReadKeyAlias(group, key)
	if !ok {
		return readKeys
	}
	out := append([][]byte(nil), readKeys...)
	return append(out, alias)
}

func (s *ShardStore) stagedVisibilityReadKeyAlias(group *ShardGroup, key []byte) ([]byte, bool) {
	if len(key) == 0 {
		return nil, false
	}
	if _, _, ok := distribution.MigrationStagedDataKeyParts(key); ok {
		return nil, false
	}
	route, g, ok := s.routeAndGroupForKey(key)
	if !ok || g != group || !routeHasStagedVisibility(route) {
		return nil, false
	}
	return distribution.MigrationStagedDataKey(route.MigrationJobID, key), true
}

// resolveSingleShardGroup returns the shard group that owns every
// mutation in the batch, or an error if the batch is cross-shard or
// references an unknown group. A nil group with nil error means "empty
// batch — caller should no-op".
func (s *ShardStore) resolveSingleShardGroup(mutations []*store.KVPairMutation) (*ShardGroup, error) {
	if len(mutations) == 0 {
		return nil, nil
	}
	firstGroup, ok := s.groupForKey(mutations[0].Key)
	if !ok || firstGroup == nil || firstGroup.Store == nil {
		return nil, store.ErrNotSupported
	}
	for i := 1; i < len(mutations); i++ {
		g, ok := s.groupForKey(mutations[i].Key)
		if !ok || g == nil || g.Store == nil {
			return nil, store.ErrNotSupported
		}
		if g != firstGroup {
			return nil, errors.WithStack(ErrCrossShardMutationBatchNotSupported)
		}
	}
	return firstGroup, nil
}

// DeletePrefixAt applies a prefix delete to every shard in the store.
func (s *ShardStore) DeletePrefixAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	routes, err := s.verifyPrefixDeleteRoutes(ctx, prefix, commitTS)
	if err != nil {
		return err
	}
	for groupID, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		deleteStagedPrefix := func(stagedPrefix []byte, stagedExcludePrefix []byte) error {
			return g.Store.DeletePrefixAt(ctx, stagedPrefix, stagedExcludePrefix, commitTS)
		}
		if err := deleteStagedVisibilityPrefixes(routesForGroupID(routes, groupID), prefix, excludePrefix, deleteStagedPrefix); err != nil {
			return err
		}
		if err := g.Store.DeletePrefixAt(ctx, prefix, excludePrefix, commitTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// DeletePrefixAtRaft is the raft-apply variant of DeletePrefixAt.
func (s *ShardStore) DeletePrefixAtRaft(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	routes, err := s.verifyPrefixDeleteRoutes(ctx, prefix, commitTS)
	if err != nil {
		return err
	}
	for groupID, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		deleteStagedPrefix := func(stagedPrefix []byte, stagedExcludePrefix []byte) error {
			return g.Store.DeletePrefixAtRaft(ctx, stagedPrefix, stagedExcludePrefix, commitTS)
		}
		if err := deleteStagedVisibilityPrefixes(routesForGroupID(routes, groupID), prefix, excludePrefix, deleteStagedPrefix); err != nil {
			return err
		}
		if err := g.Store.DeletePrefixAtRaft(ctx, prefix, excludePrefix, commitTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// DeletePrefixAtRaftAt is the raft-entry-index-aware variant. The
// caller's raft entry index applies only to the local group whose
// FSM is driving this apply; on a multi-group ShardStore, fanning
// the SAME index across other groups would corrupt their
// metaAppliedIndex. The single-group case (the common case for an
// FSM-local DeletePrefixAtRaft path) gets the correct bundling; the
// multi-group broadcast case is treated as "passive" — peer groups
// receive the prefix-delete without a meta-key bump (their own raft
// applies will catch up the index on the next mutation).
//
// In practice the FSM call sites that issue raft-DeletePrefix
// operate against a single group's store; the multi-group ShardStore
// is the receiver only when an aggregate (admin / coordinator) path
// is replaying a global FLUSHALL, which is not raft-applied.
func (s *ShardStore) DeletePrefixAtRaftAt(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS, appliedIndex uint64) error {
	routes, err := s.verifyPrefixDeleteRoutes(ctx, prefix, commitTS)
	if err != nil {
		return err
	}
	for groupID, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		deleteStagedPrefix := func(stagedPrefix []byte, stagedExcludePrefix []byte) error {
			return g.Store.DeletePrefixAtRaftAt(ctx, stagedPrefix, stagedExcludePrefix, commitTS, 0)
		}
		if err := deleteStagedVisibilityPrefixes(routesForGroupID(routes, groupID), prefix, excludePrefix, deleteStagedPrefix); err != nil {
			return err
		}
		// Pass appliedIndex through to every group. In the
		// single-group call-path (the production raft-apply case)
		// this is correct: appliedIndex IS that group's raft entry
		// index. In a hypothetical multi-group call, only one group
		// would see the matching index and the rest would treat it
		// as a non-monotonic stray write — but the rest of the
		// raft-apply contract (single FSM per raft log) makes that
		// case impossible to reach in production. Tests that
		// exercise ShardStore.DeletePrefixAtRaftAt across multiple
		// groups MUST pass appliedIndex=0 to opt out.
		if err := g.Store.DeletePrefixAtRaftAt(ctx, prefix, excludePrefix, commitTS, appliedIndex); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *ShardStore) verifyPrefixDeleteRoutes(ctx context.Context, prefix []byte, commitTS uint64) ([]distribution.Route, error) {
	if s == nil || s.engine == nil {
		return nil, nil
	}
	routeStart, routeEnd := routePrefixRange(prefix)
	routes := s.engine.GetIntersectingRoutes(routeStart, routeEnd)
	if len(routes) == 0 {
		return nil, errors.WithStack(ErrRouteCutoverPending)
	}
	verified := make([]distribution.Route, 0, len(routes))
	for _, route := range routes {
		g, ok := s.groupForID(route.GroupID)
		if !ok || g == nil || g.Store == nil {
			return nil, store.ErrNotSupported
		}
		proofRoutes, err := s.verifyPrefixDeleteRoute(ctx, g, route, routeStart, routeEnd, commitTS)
		if err != nil {
			return nil, err
		}
		verified = append(verified, proofRoutes...)
	}
	return verified, nil
}

func (s *ShardStore) verifyPrefixDeleteRoute(ctx context.Context, g *ShardGroup, route distribution.Route, routeStart []byte, routeEnd []byte, commitTS uint64) ([]distribution.Route, error) {
	proofRoutes, err := s.targetReadyRoutesForRouteRange(ctx, g, route, routeStart, routeEnd)
	if err != nil {
		return nil, err
	}
	for _, proofRoute := range proofRoutes {
		if err := verifyRouteWriteFloor(proofRoute, commitTS); err != nil {
			return nil, err
		}
	}
	return proofRoutes, nil
}

func routesForGroupID(routes []distribution.Route, groupID uint64) []distribution.Route {
	out := make([]distribution.Route, 0, len(routes))
	for _, route := range routes {
		if route.GroupID == groupID {
			out = append(out, route)
		}
	}
	return out
}

func deleteStagedVisibilityPrefixes(routes []distribution.Route, prefix []byte, excludePrefix []byte, deletePrefix func([]byte, []byte) error) error {
	seen := make(map[uint64]struct{})
	for _, route := range routes {
		if !routeHasStagedVisibility(route) {
			continue
		}
		if _, ok := seen[route.MigrationJobID]; ok {
			continue
		}
		seen[route.MigrationJobID] = struct{}{}
		stagedPrefix := distribution.MigrationStagedDataKey(route.MigrationJobID, prefix)
		var stagedExcludePrefix []byte
		if len(excludePrefix) > 0 {
			stagedExcludePrefix = distribution.MigrationStagedDataKey(route.MigrationJobID, excludePrefix)
		}
		if err := deletePrefix(stagedPrefix, stagedExcludePrefix); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
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

// LastAppliedIndex aggregates the durable applied-index across every
// shard group, returning the MIN over all groups that report one.
//
// MIN is the right aggregator because the kvFSM is per-shard in
// production — each shard's FSM independently asks "is MY group's
// applied index at least as fresh as MY group's snapshot?" — and
// ShardStore is NEVER used as the FSM's f.store in production today
// (the FSM holds a *pebbleStore directly; ShardStore is the
// coordinator-facing fanout wrapper). This method exists as a
// defensive forward in case a future refactor uses ShardStore from
// the apply path; reporting MIN guarantees the cold-start skip gate
// would refuse to skip whenever ANY group lags, matching the
// conservative "over-restore beats under-restore" rule (PR #910
// design §4).
//
// (0, false, nil) when no group reports a value — strictly-additive
// fallback per design §4.
func (s *ShardStore) LastAppliedIndex() (uint64, bool, error) {
	var (
		minIdx    uint64
		anyReport bool
	)
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		reader, ok := g.Store.(interface {
			LastAppliedIndex() (uint64, bool, error)
		})
		if !ok {
			continue
		}
		idx, present, err := reader.LastAppliedIndex()
		if err != nil {
			return 0, false, errors.WithStack(err)
		}
		if !present {
			// One group has no meta key. Conservative: report
			// missing so the cold-start skip gate falls back.
			return 0, false, nil
		}
		if !anyReport || idx < minIdx {
			minIdx = idx
		}
		anyReport = true
	}
	if !anyReport {
		return 0, false, nil
	}
	return minIdx, true, nil
}

// SetDurableAppliedIndex broadcasts the bump to every group store
// that exposes the writer seam.
//
// This is purely defensive — in production today the FSM holds a
// *pebbleStore directly; ShardStore is never f.store. Were it ever
// wired through the FSM apply path, broadcasting the same idx across
// groups would corrupt their per-group metaAppliedIndex semantics
// (each group has its own raft log with its own entry numbering).
// For that hypothetical, the test convention from
// DeletePrefixAtRaftAt applies: tests MUST pass idx=0 to opt out, or
// not use ShardStore as the writer at all. Returns the first
// per-group error.
func (s *ShardStore) SetDurableAppliedIndex(idx uint64) error {
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		writer, ok := g.Store.(interface {
			SetDurableAppliedIndex(idx uint64) error
		})
		if !ok {
			continue
		}
		if err := writer.SetDurableAppliedIndex(idx); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// WriteConflictCountsByPrefix aggregates OCC conflict counts across
// every shard group owned by this ShardStore. Per-shard counts share
// the same "<kind>|<key_prefix>" label schema, so a simple sum gives
// the node-wide view. The result is always non-nil.
func (s *ShardStore) WriteConflictCountsByPrefix() map[string]uint64 {
	out := map[string]uint64{}
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		for label, count := range g.Store.WriteConflictCountsByPrefix() {
			out[label] += count
		}
	}
	return out
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

func (s *ShardStore) Snapshot() (store.Snapshot, error) {
	return nil, store.ErrNotSupported
}

func (s *ShardStore) ExportVersions(context.Context, store.ExportVersionsOptions) (store.ExportVersionsResult, error) {
	return store.ExportVersionsResult{}, store.ErrNotSupported
}

func (s *ShardStore) ImportVersions(context.Context, store.ImportVersionsOptions) (store.ImportVersionsResult, error) {
	return store.ImportVersionsResult{}, store.ErrNotSupported
}

func (s *ShardStore) ImportVersionsRaft(context.Context, store.ImportVersionsOptions) (store.ImportVersionsResult, error) {
	return store.ImportVersionsResult{}, store.ErrNotSupported
}

func (s *ShardStore) MigrationHLCFloor(context.Context, uint64) (uint64, error) {
	return 0, store.ErrNotSupported
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
	// MVCC store lifecycle is owned by raft group runtimes; avoid closing it
	// here to prevent double-close during process shutdown.
	if closer, ok := g.Txn.(io.Closer); ok {
		if err := closer.Close(); err != nil && first == nil {
			first = errors.WithStack(err)
		}
	}

	return first
}

func (s *ShardStore) groupForKey(key []byte) (*ShardGroup, bool) {
	_, g, ok := s.routeAndGroupForKey(key)
	return g, ok
}

func (s *ShardStore) verifyExplicitGroupRoutesForRange(ctx context.Context, groupID uint64, routes []distribution.Route, start []byte, end []byte) error {
	g, ok := s.groupForID(groupID)
	if !ok || g == nil || g.Store == nil {
		return store.ErrNotSupported
	}
	routeStart, routeEnd := readinessRouteRangeForScan(start, end)
	for _, route := range routes {
		if err := s.verifyTargetReadinessForRouteRange(ctx, g, route, routeStart, routeEnd); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardStore) routeAndGroupForKey(key []byte) (distribution.Route, *ShardGroup, bool) {
	if route, ok := s.stagedVisibilityRouteForS3BucketAuxiliaryKey(key); ok {
		g, ok := s.groups[route.GroupID]
		return route, g, ok
	}
	route, ok := s.engine.GetRoute(routeKey(key))
	if !ok {
		return distribution.Route{}, nil, false
	}
	g, ok := s.groups[route.GroupID]
	return route, g, ok
}

func (s *ShardStore) stagedVisibilityRouteForS3BucketAuxiliaryKey(key []byte) (distribution.Route, bool) {
	if s == nil || s.engine == nil {
		return distribution.Route{}, false
	}
	start, end, ok := s3BucketAuxiliaryRouteRange(key)
	if !ok {
		return distribution.Route{}, false
	}
	for _, route := range s.engine.GetIntersectingRoutes(start, end) {
		if routeHasStagedVisibility(route) {
			return route, true
		}
	}
	return distribution.Route{}, false
}

func (s *ShardStore) proxyRawGet(ctx context.Context, g *ShardGroup, key []byte, ts uint64, groupID uint64, readRouteVersion uint64) ([]byte, error) {
	engine := engineForGroup(g)
	if engine == nil {
		return nil, store.ErrKeyNotFound
	}
	addr := leaderAddrFromEngine(engine)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, proxyForwardTimeout)
	defer cancel()
	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts, GroupId: groupID, ReadRouteVersion: readRouteVersion})
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

func (s *ShardStore) proxyRawScanAt(
	ctx context.Context,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	groupID uint64,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	engine := engineForGroup(g)
	if engine == nil {
		return nil, store.ErrNotSupported
	}
	addr := leaderAddrFromEngine(engine)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, proxyForwardTimeout)
	defer cancel()
	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:           start,
		EndKey:             end,
		Limit:              int64(limit),
		Ts:                 ts,
		Reverse:            reverse,
		GroupId:            groupID,
		ReadRouteVersion:   readRouteVersion,
		RouteStart:         bytes.Clone(routeStart),
		RouteEnd:           bytes.Clone(routeEnd),
		RouteBoundsPresent: routeScanBoundsPresent(routeStart, routeEnd),
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

func (s *ShardStore) proxyRawScanKeysAt(
	ctx context.Context,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	groupID uint64,
	readRouteVersion uint64,
) ([][]byte, error) {
	engine := engineForGroup(g)
	if engine == nil {
		return nil, store.ErrNotSupported
	}
	addr := leaderAddrFromEngine(engine)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := s.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, proxyForwardTimeout)
	defer cancel()
	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:         start,
		EndKey:           end,
		Limit:            int64(limit),
		Ts:               ts,
		GroupId:          groupID,
		ReadRouteVersion: readRouteVersion,
		KeysOnly:         true,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([][]byte, 0, len(resp.Kv))
	for _, kvp := range resp.Kv {
		if kvp == nil {
			continue
		}
		out = append(out, bytes.Clone(kvp.Key))
	}
	return out, nil
}

var _ store.MVCCStore = (*ShardStore)(nil)
