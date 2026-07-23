package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const proxyForwardTimeout = 5 * time.Second

// ShardStore routes MVCC reads to shard-specific stores and proxies to leaders when needed.
type ShardStore struct {
	engine            *distribution.Engine
	groups            map[uint64]*ShardGroup
	partitionResolver PartitionResolver

	connCache GRPCConnCache
}

var (
	ErrCrossShardMutationBatchNotSupported = errors.New("cross-shard mutation batches are not supported")
	ErrFilesystemPlacementTargetNotFound   = errors.New("filesystem placement target group has no routable home slot")
)

// NewShardStore creates a sharded MVCC store wrapper.
func NewShardStore(engine *distribution.Engine, groups map[uint64]*ShardGroup) *ShardStore {
	return &ShardStore{
		engine: engine,
		groups: groups,
	}
}

// WithPartitionResolver installs the same partition-keyspace resolver used by
// ShardedCoordinator. ShardStore keeps normal byte-range routing for ordinary
// calls, but backup scanners need the resolver to decide which physical group
// owns partition-routed keys discovered while scanning every group.
func (s *ShardStore) WithPartitionResolver(r PartitionResolver) *ShardStore {
	if s != nil {
		s.partitionResolver = r
	}
	return s
}

// FilesystemGroupForHome resolves the group that owns one file-home route.
func (s *ShardStore) FilesystemGroupForHome(homeSlot uint64, inode uint64) (uint64, bool) {
	if s == nil || s.engine == nil {
		return 0, false
	}
	route, ok := s.engine.GetRoute(fskeys.ChunkRouteKey(homeSlot, inode))
	if !ok {
		return 0, false
	}
	return route.GroupID, true
}

// FilesystemGroupIDs returns every physical group that may retain filesystem
// chunks, including stale copies no longer owned by the current route catalog.
func (s *ShardStore) FilesystemGroupIDs() []uint64 {
	if s == nil {
		return nil
	}
	groupIDs := make([]uint64, 0, len(s.groups))
	for groupID := range s.groups {
		groupIDs = append(groupIDs, groupID)
	}
	slices.Sort(groupIDs)
	return groupIDs
}

// ResolveFilesystemHomeSlot finds a home token whose file route belongs to
// targetGroup. It derives candidates from current route boundaries and verifies
// each candidate against the live catalog before returning it.
func (s *ShardStore) ResolveFilesystemHomeSlot(targetGroup uint64, inode uint64) (uint64, error) {
	if s == nil || s.engine == nil || targetGroup == 0 {
		return 0, ErrFilesystemPlacementTargetNotFound
	}
	prefix := fskeys.ChunkRouteAllPrefix()
	routes := s.engine.GetIntersectingRoutes(prefix, prefixScanEnd(prefix))
	homes := filesystemHomeCandidates(routes, targetGroup, prefix)
	for _, home := range homes {
		groupID, ok := s.FilesystemGroupForHome(home, inode)
		if ok && groupID == targetGroup {
			return home, nil
		}
	}
	return 0, errors.Wrapf(ErrFilesystemPlacementTargetNotFound, "group_id=%d inode=%d", targetGroup, inode)
}

func filesystemHomeCandidates(routes []distribution.Route, targetGroup uint64, prefix []byte) []uint64 {
	candidates := map[uint64]struct{}{targetGroup: {}}
	for _, route := range routes {
		if route.GroupID != targetGroup {
			continue
		}
		addFilesystemRouteHomeCandidates(candidates, route, prefix)
	}
	homes := make([]uint64, 0, len(candidates))
	for home := range candidates {
		homes = append(homes, home)
	}
	slices.Sort(homes)
	return homes
}

func addFilesystemRouteHomeCandidates(candidates map[uint64]struct{}, route distribution.Route, prefix []byte) {
	if home, ok := filesystemHomeCandidateFromBound(route.Start, false); ok {
		candidates[home] = struct{}{}
		if home < math.MaxUint64 {
			candidates[home+1] = struct{}{}
		}
	} else if bytes.Compare(route.Start, prefix) <= 0 {
		candidates[0] = struct{}{}
	}
	if home, ok := filesystemHomeCandidateFromBound(route.End, true); ok {
		candidates[home] = struct{}{}
		if home > 0 {
			candidates[home-1] = struct{}{}
		}
	} else if route.End == nil || bytes.Compare(route.End, prefixScanEnd(prefix)) >= 0 {
		candidates[math.MaxUint64] = struct{}{}
	}
}

func filesystemHomeCandidateFromBound(bound []byte, upper bool) (uint64, bool) {
	prefix := fskeys.ChunkRouteAllPrefix()
	if !bytes.HasPrefix(bound, prefix) || len(bound) == len(prefix) {
		return 0, false
	}
	rest := bound[len(prefix):]
	var encoded [8]byte
	if upper {
		for i := range encoded {
			encoded[i] = math.MaxUint8
		}
	}
	copy(encoded[:], rest[:min(len(rest), len(encoded))])
	return binary.BigEndian.Uint64(encoded[:]), true
}

func (s *ShardStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}

	// Some tests use ShardStore without raft; in that case serve reads locally.
	if engineForGroup(g) == nil {
		return s.localGetAt(ctx, g, key, ts)
	}

	// Wait for a leader read fence before serving from local state.
	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.leaderGetAt(ctx, g, key, ts)
	}
	return s.proxyRawGet(ctx, g, key, ts, 0)
}

// GetGroupAt reads a key from the explicitly selected Raft group.
// It is for keyspaces whose owner is resolved outside the byte-range
// engine (for example SQS HT-FIFO's (queue, partition) resolver).
func (s *ShardStore) GetGroupAt(ctx context.Context, groupID uint64, key []byte, ts uint64) ([]byte, error) {
	g, ok := s.groupForID(groupID)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}

	return s.getGroupAt(ctx, g, key, ts, groupID)
}

func (s *ShardStore) getRouteAt(ctx context.Context, route distribution.Route, key []byte, ts uint64) ([]byte, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g.Store == nil {
		return nil, store.ErrKeyNotFound
	}
	return s.getGroupAt(ctx, g, key, ts, route.GroupID)
}

func (s *ShardStore) getGroupAt(ctx context.Context, g *ShardGroup, key []byte, ts uint64, groupID uint64) ([]byte, error) {
	if engineForGroup(g) == nil {
		return s.localGetAt(ctx, g, key, ts)
	}
	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.leaderGetAt(ctx, g, key, ts)
	}
	return s.proxyRawGet(ctx, g, key, ts, groupID)
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
	g, ok := s.groupForKey(key)
	if !ok || g.Store == nil {
		return false, nil
	}
	// engineForGroup may be nil in test fixtures that wire ShardStore
	// without raft; preserve the existing local-only fallback there.
	engine := engineForGroup(g)
	if engine == nil {
		exists, err := g.Store.CommittedVersionAt(ctx, key, commitTS)
		if err != nil {
			return false, errors.WithStack(err)
		}
		return exists, nil
	}
	if !isLinearizableRaftLeader(ctx, engine) && !tryEngineLinearizableFence(ctx, engine) {
		// Not the linearizable leader for this group AND the ReadIndex
		// fence failed (no leader reachable, ctx canceled). Fall back to
		// (false, nil); the adapter's resolveListMeta path takes over via
		// the leader-fenced ScanAt/GetAt and returns a valid current-Len
		// serialization.
		return false, nil
	}
	exists, err := g.Store.CommittedVersionAt(ctx, key, commitTS)
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
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	routes, clampToRoutes := s.routesForForwardScan(start, end)
	return s.scanRoutesAtSorted(ctx, routes, start, end, limit, ts, clampToRoutes)
}

func (s *ShardStore) ScanKeysAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	if limit <= 0 {
		return [][]byte{}, nil
	}

	routes, clampToRoutes := s.routesForForwardScan(start, end)
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
	routes, clampToRoutes := s.routesForForwardScan(start, end)
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
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	return s.scanRouteAtDirection(ctx, distribution.Route{GroupID: groupID}, start, end, limit, ts, false, true)
}

// ReverseScanGroupAt reverse-scans a range on the explicitly selected Raft group.
func (s *ShardStore) ReverseScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}
	return s.scanRouteAtDirection(ctx, distribution.Route{GroupID: groupID}, start, end, limit, ts, true, true)
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
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	routes, clampToRoutes := s.routesForReverseScan(start, end)
	out, err := s.reverseScanRoutesAt(ctx, routes, start, end, limit, ts, clampToRoutes)
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
	routes, clampToRoutes := s.routesForReverseScan(start, end)
	if len(routes) != 1 || clampToRoutes {
		kvs, err := s.ReverseScanAt(ctx, start, end, visibleLimit, ts)
		return kvs, false, err
	}
	return s.scanRouteAtDirectionPhysicalLimit(ctx, routes[0], start, end, visibleLimit, physicalLimit, ts, true)
}

func (s *ShardStore) routesForForwardScan(start []byte, end []byte) ([]distribution.Route, bool) {
	return s.routesForScan(start, end, true)
}

func (s *ShardStore) routesForReverseScan(start []byte, end []byte) ([]distribution.Route, bool) {
	return s.routesForScan(start, end, true)
}

func (s *ShardStore) routesForScan(start []byte, end []byte, useFilesystemChunkRoutes bool) ([]distribution.Route, bool) {
	if routeStart, routeEnd, ok := s3keys.ManifestScanRouteBounds(start, end); ok {
		return s.engine.GetIntersectingRoutes(routeStart, routeEnd), false
	}
	if routes, ok := s.routesForFilesystemUsageScan(start, end); ok {
		return routes, false
	}
	if useFilesystemChunkRoutes {
		if routes, ok := s.routesForFilesystemChunkScan(start, end); ok {
			return routes, false
		}
	}
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

func (s *ShardStore) routesForFilesystemUsageScan(start []byte, end []byte) ([]distribution.Route, bool) {
	if !filesystemUsageScanOverlap(start, end) {
		return nil, false
	}
	// Keep every captured range so backup materialization remains pinned to the
	// catalog snapshot. Unclamped scan dispatch de-duplicates these by group.
	return s.engine.GetIntersectingRoutes(nil, nil), true
}

func (s *ShardStore) routesForFilesystemChunkScan(start []byte, end []byte) ([]distribution.Route, bool) {
	if routeStart, routeEnd, ok := fskeys.ChunkScanRouteBounds(start, end); ok {
		return s.engine.GetIntersectingRoutes(routeStart, routeEnd), true
	}
	chunkStart, chunkEnd, ok := filesystemChunkScanOverlap(start, end)
	if !ok {
		return nil, false
	}
	routeStart, routeEnd, ok := fskeys.ChunkScanRouteBounds(chunkStart, chunkEnd)
	if !ok {
		return nil, false
	}
	// Raw scans can continue from the chunk keyspace into later filesystem
	// families, so include both raw and virtual chunk route groups rather than
	// narrowing the scan to chunks only.
	routes := s.engine.GetIntersectingRoutes(start, end)
	routes = append(routes, s.engine.GetIntersectingRoutes(routeStart, routeEnd)...)
	return routes, true
}

func filesystemUsageScanOverlap(start []byte, end []byte) bool {
	usageStart := fskeys.UsageRouteAllPrefix()
	usageEnd := prefixScanEnd(usageStart)
	if len(end) > 0 && bytes.Compare(end, usageStart) <= 0 {
		return false
	}
	if len(start) > 0 && bytes.Compare(start, usageEnd) >= 0 {
		return false
	}
	return true
}

func filesystemChunkScanOverlap(start []byte, end []byte) ([]byte, []byte, bool) {
	chunkStart := fskeys.ChunkAllPrefix()
	chunkEnd := prefixScanEnd(chunkStart)
	if len(end) > 0 && bytes.Compare(end, chunkStart) <= 0 {
		return nil, nil, false
	}
	if len(start) > 0 && bytes.Compare(start, chunkEnd) >= 0 {
		return nil, nil, false
	}
	overlapStart := chunkStart
	if len(start) > 0 && bytes.Compare(start, chunkStart) > 0 {
		overlapStart = start
	}
	overlapEnd := chunkEnd
	if len(end) > 0 && bytes.Compare(end, chunkEnd) < 0 {
		overlapEnd = end
	}
	if bytes.Compare(overlapStart, overlapEnd) >= 0 {
		return nil, nil, false
	}
	return overlapStart, overlapEnd, true
}

func (s *ShardStore) scanRoutesAt(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, clampToRoutes bool) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	seenGroups := make(map[uint64]struct{})
	filterUsageOwners := !clampToRoutes && filesystemUsageScanOverlap(start, end)
	for _, route := range routes {
		if !clampToRoutes {
			if _, seen := seenGroups[route.GroupID]; seen {
				continue
			}
			seenGroups[route.GroupID] = struct{}{}
		}
		scanStart := start
		scanEnd := end
		explicitGroup := !clampToRoutes
		if clampToRoutes {
			scanStart = clampScanStart(start, route.Start)
			scanEnd = clampScanEnd(end, route.End)
		}

		var kvs []*store.KVPair
		var err error
		if filterUsageOwners {
			kvs, err = s.scanRouteAtWithFilesystemUsageOwnerFilter(
				ctx, route, scanStart, scanEnd, limit, ts, false, explicitGroup,
			)
		} else {
			kvs, err = s.scanRouteAtDirection(ctx, route, scanStart, scanEnd, limit, ts, false, explicitGroup)
		}
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
		out = mergeAndTrimScanResults(out, kvs, limit)
	}
	return out, nil
}

func (s *ShardStore) scanRoutesAtSorted(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, clampToRoutes bool) ([]*store.KVPair, error) {
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

func (s *ShardStore) scanKeyRoutesAt(ctx context.Context, routes []distribution.Route, start []byte, end []byte, limit int, ts uint64, clampToRoutes bool) ([][]byte, error) {
	out := make([][]byte, 0)
	seenGroups := make(map[uint64]struct{})
	filterUsageOwners := !clampToRoutes && filesystemUsageScanOverlap(start, end)
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

		var keys [][]byte
		var err error
		if filterUsageOwners {
			keys, err = s.scanKeyRouteAtWithFilesystemUsageOwnerFilter(
				ctx, route, scanStart, scanEnd, limit, ts,
			)
		} else {
			keys, err = s.scanKeyRouteAt(ctx, route, scanStart, scanEnd, limit, ts)
		}
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

func (s *ShardStore) filterFilesystemUsageKVsForGroup(kvs []*store.KVPair, groupID uint64) []*store.KVPair {
	write := 0
	for _, pair := range kvs {
		if pair == nil || !fskeys.IsUsageRouteKey(pair.Key) || s.filesystemUsageKeyOwnedByGroup(pair.Key, groupID) {
			kvs[write] = pair
			write++
		}
	}
	clear(kvs[write:])
	return kvs[:write]
}

func (s *ShardStore) filterFilesystemUsageKeysForGroup(keys [][]byte, groupID uint64) [][]byte {
	write := 0
	for _, key := range keys {
		if !fskeys.IsUsageRouteKey(key) || s.filesystemUsageKeyOwnedByGroup(key, groupID) {
			keys[write] = key
			write++
		}
	}
	clear(keys[write:])
	return keys[:write]
}

func (s *ShardStore) filesystemUsageKeyOwnedByGroup(key []byte, groupID uint64) bool {
	owner, ok := s.engine.GetRoute(routeKey(key))
	return ok && owner.GroupID == groupID
}

//nolint:cyclop // Owner-filtered pagination must advance both scan directions without returning stale rows.
func (s *ShardStore) scanRouteAtWithFilesystemUsageOwnerFilter(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	explicitGroup bool,
) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	out := make([]*store.KVPair, 0, limit)
	cursorStart := start
	cursorEnd := end
	for len(out) < limit {
		page, err := s.scanRouteAtDirection(
			ctx, route, cursorStart, cursorEnd, limit, ts, reverse, explicitGroup,
		)
		if err != nil {
			return nil, err
		}
		pageLen := len(page)
		advanceKey := lastKVKey(page)
		page = s.filterFilesystemUsageKVsForGroup(page, route.GroupID)
		if reverse {
			out = mergeAndTrimReverseScanResults(out, page, limit)
		} else {
			out = mergeAndTrimScanResults(out, page, limit)
		}
		if len(out) >= limit || pageLen < limit || advanceKey == nil {
			break
		}
		if reverse {
			if len(cursorStart) > 0 && bytes.Compare(advanceKey, cursorStart) <= 0 {
				break
			}
			cursorEnd = advanceKey
			continue
		}
		cursorStart = nextScanCursor(advanceKey)
		if cursorEnd != nil && bytes.Compare(cursorStart, cursorEnd) >= 0 {
			break
		}
	}
	return out, nil
}

func (s *ShardStore) scanKeyRouteAtWithFilesystemUsageOwnerFilter(
	ctx context.Context,
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
		page, err := s.scanKeyRouteAt(ctx, route, cursor, end, limit, ts)
		if err != nil {
			return nil, err
		}
		pageLen := len(page)
		advanceKey := lastScanKey(page)
		page = s.filterFilesystemUsageKeysForGroup(page, route.GroupID)
		out = mergeAndTrimScanKeys(out, page, limit)
		if len(out) >= limit || pageLen < limit || advanceKey == nil {
			break
		}
		cursor = nextScanCursor(advanceKey)
		if end != nil && bytes.Compare(cursor, end) >= 0 {
			break
		}
	}
	return out, nil
}

func (s *ShardStore) reverseScanRoutesAt(
	ctx context.Context,
	routes []distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	clampToRoutes bool,
) ([]*store.KVPair, error) {
	out := make([]*store.KVPair, 0)
	seenGroups := make(map[uint64]struct{})
	filterUsageOwners := !clampToRoutes && filesystemUsageScanOverlap(start, end)
	for i := len(routes) - 1; i >= 0; i-- {
		route := routes[i]
		if clampToRoutes {
			kvs, done, err := s.clampedReverseScanRouteAt(ctx, route, start, end, limit, len(out), ts)
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
		// De-duplicate by GroupID: after a range split both halves share the same
		// GroupID (same backing shard store), so only scan each group once.
		if _, seen := seenGroups[route.GroupID]; seen {
			continue
		}
		seenGroups[route.GroupID] = struct{}{}
		var kvs []*store.KVPair
		var err error
		if filterUsageOwners {
			kvs, err = s.scanRouteAtWithFilesystemUsageOwnerFilter(
				ctx, route, start, end, limit, ts, true, true,
			)
		} else {
			kvs, err = s.scanRouteAtDirection(ctx, route, start, end, limit, ts, true, true)
		}
		if err != nil {
			return nil, err
		}
		out = mergeAndTrimReverseScanResults(out, kvs, limit)
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
		return s.scanKeysRouteAtLeader(ctx, g, start, end, limit, ts)
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
			keys, err := s.scanLockOnlyKeysAtLeader(ctx, g, cursor, end, ts, limit)
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
		kvs, err := s.resolveScanLocks(ctx, g, keyKVs, lockKVs, ts)
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
	kvs, err := s.resolveScanLocks(ctx, g, nil, lockKVs, ts)
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
	return scanKeysWithRefill(start, end, limit, func(cursor []byte, pageLimit int) ([][]byte, error) {
		return s.proxyRawScanKeysAt(ctx, g, cursor, end, pageLimit, ts, groupID)
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

func (s *ShardStore) clampedReverseScanRouteAt(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	currentLen int,
	ts uint64,
) ([]*store.KVPair, bool, error) {
	if currentLen >= limit {
		return nil, true, nil
	}

	scanStart := clampScanStart(start, route.Start)
	scanEnd := clampScanEnd(end, route.End)
	kvs, err := s.scanRouteAtDirection(ctx, route, scanStart, scanEnd, limit-currentLen, ts, true, false)
	if err != nil {
		return nil, false, err
	}
	return kvs, false, nil
}

func (s *ShardStore) scanRouteAtDirection(
	ctx context.Context,
	route distribution.Route,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	explicitGroup bool,
) ([]*store.KVPair, error) {
	g, ok := s.groupForID(route.GroupID)
	if !ok || g == nil || g.Store == nil {
		return nil, nil
	}

	if !reverse {
		return s.scanRouteAtForward(ctx, route, g, start, end, limit, ts)
	}

	if engineForGroup(g) == nil {
		kvs, err := s.scanRouteLocal(ctx, g, start, end, limit, ts, reverse)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return filterTxnInternalKVs(kvs), nil
	}

	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.scanRouteAtLeader(ctx, g, start, end, limit, ts, reverse)
	}

	var groupID uint64
	if explicitGroup {
		groupID = route.GroupID
	}
	kvs, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts, reverse, groupID)
	if err != nil {
		return nil, err
	}
	// The leader's RawScanAt is expected to perform lock resolution and filtering
	// via ShardStore.ScanAt, so avoid N+1 proxy gets here.
	return filterTxnInternalKVs(kvs), nil
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
) ([]*store.KVPair, error) {
	if limit <= 0 {
		return []*store.KVPair{}, nil
	}

	out := make([]*store.KVPair, 0, limit)
	cursor := start
	for len(out) < limit {
		page, err := s.scanRouteAtForwardPage(ctx, route, g, cursor, end, limit, ts)
		if err != nil {
			return nil, err
		}
		out = mergeAndTrimScanResults(out, page.kvs, limit)
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
	return out, nil
}

func (s *ShardStore) scanRouteAtForwardPage(
	ctx context.Context,
	route distribution.Route,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
) (scanRoutePage, error) {
	engine := engineForGroup(g)
	if engine == nil {
		raw, err := s.scanRouteLocal(ctx, g, start, end, limit, ts, false)
		if err != nil {
			return scanRoutePage{}, errors.WithStack(err)
		}
		return scanRoutePage{
			kvs:        filterTxnInternalKVs(raw),
			advanceKey: lastKVKey(raw),
			full:       len(raw) >= limit,
		}, nil
	}

	if isLinearizableRaftLeader(ctx, engine) {
		raw, err := g.Store.ScanAt(ctx, start, end, limit, ts)
		if err != nil {
			return scanRoutePage{}, errors.WithStack(err)
		}
		lockStart, lockEnd := scanLockBoundsForKVs(raw, start, end, limit)
		lockKVs, err := scanTxnLockRangeAt(ctx, g, lockStart, lockEnd, ts, limit)
		if err != nil {
			return scanRoutePage{}, err
		}
		kvs, err := s.resolveScanLocks(ctx, g, raw, lockKVs, ts)
		if err != nil {
			return scanRoutePage{}, err
		}
		return scanRoutePage{
			kvs:        filterTxnInternalKVs(kvs),
			advanceKey: lastKVKey(raw),
			full:       len(raw) >= limit,
		}, nil
	}

	raw, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts, false, route.GroupID)
	if err != nil {
		return scanRoutePage{}, err
	}
	return scanRoutePage{
		kvs:        filterTxnInternalKVs(raw),
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

	if engineForGroup(g) == nil {
		kvs, limitReached, err := scanLocalPhysicalLimit(ctx, g.Store, start, end, visibleLimit, physicalLimit, ts, reverse)
		if err != nil {
			return nil, limitReached, errors.WithStack(err)
		}
		return filterTxnInternalKVs(kvs), limitReached, nil
	}

	if isLinearizableRaftLeader(ctx, engineForGroup(g)) {
		return s.scanRouteAtLeaderPhysicalLimit(ctx, g, start, end, visibleLimit, physicalLimit, ts, reverse)
	}

	// RawScanAt cannot enforce physicalLimit, so report truncation and let
	// callers fail closed instead of proxying an unbounded physical scan.
	return nil, true, nil
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
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, error) {
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
	start []byte,
	end []byte,
	visibleLimit int,
	physicalLimit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, bool, error) {
	kvs, limitReached, err := scanLocalPhysicalLimit(ctx, g.Store, start, end, visibleLimit, physicalLimit, ts, reverse)
	if err != nil {
		return nil, limitReached, errors.WithStack(err)
	}
	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, start, end, visibleLimit, reverse)
	lockKVs, err := scanTxnLockRangeAt(ctx, g, lockStart, lockEnd, ts, visibleLimit)
	if err != nil {
		return nil, limitReached, err
	}
	resolved, err := s.resolveScanLocks(ctx, g, kvs, lockKVs, ts)
	return resolved, limitReached, err
}

func (s *ShardStore) scanRouteAtLeader(
	ctx context.Context,
	g *ShardGroup,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
) ([]*store.KVPair, error) {
	var (
		kvs []*store.KVPair
		err error
	)
	if reverse {
		kvs, err = g.Store.ReverseScanAt(ctx, start, end, limit, ts)
	} else {
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
	return s.resolveScanLocks(ctx, g, kvs, lockKVs, ts)
}

func scanLockBoundsForKVs(kvs []*store.KVPair, scanStart []byte, scanEnd []byte, limit int) ([]byte, []byte) {
	return scanLockBoundsForKVsDirection(kvs, scanStart, scanEnd, limit, false)
}

func scanLockBoundsForKVsDirection(kvs []*store.KVPair, scanStart []byte, scanEnd []byte, limit int, reverse bool) ([]byte, []byte) {
	if len(kvs) < limit {
		return scanStart, scanEnd
	}
	_, lastUserKey, ok := observedScanUserBounds(kvs)
	if ok {
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
	if !isTxnInternalKey(kvp.Key) {
		return kvp.Key, true
	}
	return txnUserKeyFromLockKey(kvp.Key)
}

func mergeAndTrimScanResults(out []*store.KVPair, kvs []*store.KVPair, limit int) []*store.KVPair {
	if len(kvs) == 0 {
		return out
	}
	out = append(out, kvs...)
	if len(out) <= limit {
		return out
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
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
	if len(kvs) == 0 {
		return out
	}
	out = append(out, kvs...)
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) > 0
	})
	if len(out) <= limit {
		return out
	}
	clear(out[limit:])
	return out[:limit]
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

	if engineForGroup(g) == nil {
		ts, exists, err := g.Store.LatestCommitTS(ctx, key)
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
	if err := applyScanLockResolutions(ctx, g, plan); err != nil {
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
	return errors.WithStack(group.Store.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS))
}

// ApplyMutationsRaft is the raft-apply variant; see store.MVCCStore for the
// durability contract. Only the FSM may call this method.
func (s *ShardStore) ApplyMutationsRaft(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	group, err := s.resolveSingleShardGroup(mutations)
	if err != nil || group == nil {
		return err
	}
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
	return errors.WithStack(group.Store.ApplyMutationsRaftAt(ctx, mutations, readKeys, startTS, commitTS, appliedIndex))
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
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
		}
		if err := g.Store.DeletePrefixAt(ctx, prefix, excludePrefix, commitTS); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// DeletePrefixAtRaft is the raft-apply variant of DeletePrefixAt.
func (s *ShardStore) DeletePrefixAtRaft(ctx context.Context, prefix []byte, excludePrefix []byte, commitTS uint64) error {
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
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
	for _, g := range s.groups {
		if g == nil || g.Store == nil {
			continue
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
	route, ok := s.engine.GetRoute(routeKey(key))
	if !ok {
		return nil, false
	}
	g, ok := s.groups[route.GroupID]
	return g, ok
}

func (s *ShardStore) proxyRawGet(ctx context.Context, g *ShardGroup, key []byte, ts uint64, groupID uint64) ([]byte, error) {
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
	resp, err := cli.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts, GroupId: groupID})
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
		StartKey: start,
		EndKey:   end,
		Limit:    int64(limit),
		Ts:       ts,
		Reverse:  reverse,
		GroupId:  groupID,
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
		StartKey: start,
		EndKey:   end,
		Limit:    int64(limit),
		Ts:       ts,
		GroupId:  groupID,
		KeysOnly: true,
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
