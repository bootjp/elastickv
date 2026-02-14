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
		val, err := g.Store.GetAt(ctx, key, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return val, nil
	}

	// Verify leadership with a quorum before serving reads from local state to
	// avoid stale results from a deposed leader.
	if g.Raft.State() == raft.Leader {
		if err := g.Raft.VerifyLeader().Error(); err == nil {
			val, err := g.Store.GetAt(ctx, key, ts)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return val, nil
		}
	}
	return s.proxyRawGet(ctx, g, key, ts)
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
		return kvs, nil
	}

	// Reads should come from the shard's leader to avoid returning stale or
	// incomplete results when this node is a follower for a given shard.
	if g.Raft.State() == raft.Leader {
		if err := g.Raft.VerifyLeader().Error(); err == nil {
			kvs, err := g.Store.ScanAt(ctx, start, end, limit, ts)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return kvs, nil
		}
	}

	kvs, err := s.proxyRawScanAt(ctx, g, start, end, limit, ts)
	if err != nil {
		return nil, err
	}
	return kvs, nil
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
			return errors.WithStack(errors.Mark(
				errors.Wrap(store.ErrNotSupported, ErrCrossShardMutationBatchNotSupported.Error()),
				ErrCrossShardMutationBatchNotSupported,
			))
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
	if resp.Value == nil {
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
