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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ShardStore routes MVCC reads to shard-specific stores and proxies to leaders when needed.
type ShardStore struct {
	engine *distribution.Engine
	groups map[uint64]*ShardGroup
}

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
	if g.Raft != nil && g.Raft.State() == raft.Leader {
		val, err := g.Store.GetAt(ctx, key, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return val, nil
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

	// Get only the routes whose ranges intersect with [start, end)
	intersectingRoutes := s.engine.GetIntersectingRoutes(start, end)

	var out []*store.KVPair
	for _, route := range intersectingRoutes {
		g, ok := s.groups[route.GroupID]
		if !ok || g == nil || g.Store == nil {
			continue
		}
		kvs, err := g.Store.ScanAt(ctx, start, end, limit, ts)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		out = append(out, kvs...)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Key, out[j].Key) < 0
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
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
	ts, exists, err := g.Store.LatestCommitTS(ctx, key)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return ts, exists, nil
}

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
			// Mixed-shard mutation batches are not supported.
			return store.ErrNotSupported
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
		if g == nil || g.Store == nil {
			continue
		}
		if err := g.Store.Close(); err != nil && first == nil {
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

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

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

var _ store.MVCCStore = (*ShardStore)(nil)
