package kv

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// recordingSampler is a keyviz.Sampler that records every Observe
// call so tests can assert dispatch wiring fires once per resolved
// (RouteID, mutation key) pair.
type recordingSampler struct {
	mu    sync.Mutex
	calls []sampleCall
}

type sampleCall struct {
	routeID  uint64
	op       keyviz.Op
	label    keyviz.Label
	key      []byte
	keyLen   int
	valueLen int
}

func (r *recordingSampler) Observe(routeID uint64, key []byte, op keyviz.Op, valueLen int, label keyviz.Label) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Copy the key: the sampler contract lets callers reuse the buffer,
	// and the sub-range bucketer depends on the exact bytes, so the test
	// asserts the forwarded bytes (not just length) survived the
	// keyLen->key signature change.
	r.calls = append(r.calls, sampleCall{
		routeID:  routeID,
		op:       op,
		label:    label,
		key:      append([]byte(nil), key...),
		keyLen:   len(key),
		valueLen: valueLen,
	})
}

func (r *recordingSampler) snapshot() []sampleCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sampleCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// TestShardedCoordinatorObservesEveryDispatchedMutation pins the
// keyviz wiring contract: every successfully-routed mutation in a
// non-txn dispatch produces exactly one Observe call carrying the
// resolved RouteID, OpWrite, and the mutation's key/value lengths.
func TestShardedCoordinatorObservesEveryDispatchedMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 101, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "kv-sampler-g1", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)
	s2 := store.NewMVCCStore()
	r2, stop2 := newSingleRaft(t, "kv-sampler-g2", NewKvFSMWithHLC(s2, NewHLC()))
	t.Cleanup(stop2)

	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
		2: {Engine: r2, Store: s2, Txn: NewLeaderProxyWithEngine(r2)},
	}
	shardStore := NewShardStore(engine, groups)

	rec := &recordingSampler{}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), shardStore).WithSampler(rec)

	// Cross-shard non-txn dispatch: "b" → group 1, "x" → group 2.
	ops := &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("val-b")},
			{Op: Put, Key: []byte("x"), Value: []byte("val-x-longer")},
		},
	}
	_, err := coord.Dispatch(ctx, ops)
	require.NoError(t, err)

	calls := rec.snapshot()
	require.Len(t, calls, 2, "expected one Observe per mutation")

	// groupMutations iterates reqs in order, so call[i] matches
	// elem[i]. Resolve via routeKey(elem.Key) so the test mirrors
	// production's routing transform — important for keys that
	// normalize through internal-prefix handling.
	for i, elem := range ops.Elems {
		route, ok := engine.GetRoute(routeKey(elem.Key))
		require.True(t, ok)
		require.Equal(t, sampleCall{
			routeID:  route.RouteID,
			op:       keyviz.OpWrite,
			label:    keyviz.LabelLegacy,
			key:      append([]byte(nil), elem.Key...),
			keyLen:   len(elem.Key),
			valueLen: len(elem.Value),
		}, calls[i], "Observe call %d for key %q", i, elem.Key)
	}
}

func TestShardedCoordinatorObservesForwardedRequests(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 100, Start: []byte("a"), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 101, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {},
		2: {},
	}
	rec := &recordingSampler{}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), nil).WithSampler(rec)

	coord.ObserveForwardedRequests([]*pb.Request{
		{
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("left")},
			},
		},
		{
			IsTxn: true,
			Phase: pb.Phase_NONE,
			Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(TxnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("x")})},
				{Op: pb.Op_PUT, Key: []byte("x"), Value: []byte("right")},
			},
		},
	})

	calls := rec.snapshot()
	require.Len(t, calls, 2)
	require.Equal(t, sampleCall{
		routeID:  100,
		op:       keyviz.OpWrite,
		label:    keyviz.LabelLegacy,
		key:      []byte("b"),
		keyLen:   1,
		valueLen: len("left"),
	}, calls[0])
	require.Equal(t, sampleCall{
		routeID:  101,
		op:       keyviz.OpWrite,
		label:    keyviz.LabelLegacy,
		key:      []byte("x"),
		keyLen:   1,
		valueLen: len("right"),
	}, calls[1])
}

// TestShardedCoordinatorWithoutSamplerStaysSafe pins the nil-safe
// contract: a coordinator without WithSampler (interface-nil
// c.sampler) and one wired with a typed-nil *MemSampler must both
// dispatch successfully without observing anything. The "no
// WithSampler" subcase additionally asserts c.sampler stays the
// zero interface value so a future refactor that silently
// initialises the field would fail this guard.
func TestShardedCoordinatorWithoutSamplerStaysSafe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, tc := range []struct {
		name         string
		opt          func(*ShardedCoordinator) *ShardedCoordinator
		wantNilField bool
	}{
		{
			name:         "no WithSampler call",
			opt:          func(c *ShardedCoordinator) *ShardedCoordinator { return c },
			wantNilField: true,
		},
		{
			name: "typed-nil *MemSampler",
			opt: func(c *ShardedCoordinator) *ShardedCoordinator {
				return c.WithSampler((*keyviz.MemSampler)(nil))
			},
			wantNilField: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine := distribution.NewEngine()
			engine.UpdateRoute([]byte("a"), nil, 1)

			s1 := store.NewMVCCStore()
			r1, stop1 := newSingleRaft(t, "kv-sampler-nilsafe-"+tc.name, NewKvFSMWithHLC(s1, NewHLC()))
			t.Cleanup(stop1)
			groups := map[uint64]*ShardGroup{
				1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
			}
			coord := tc.opt(NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups)))

			if tc.wantNilField {
				require.Nil(t, coord.sampler, "expected sampler field to be unset when WithSampler is never called")
			}

			ops := &OperationGroup[OP]{
				Elems: []*Elem[OP]{{Op: Put, Key: []byte("b"), Value: []byte("v")}},
			}
			_, err := coord.Dispatch(ctx, ops)
			require.NoError(t, err)
		})
	}
}

// TestShardedCoordinatorObservesLeaseAndLinearizableReads pins the
// read-side wiring: LeaseReadForKey and LinearizableReadForKey each
// produce one Observe(routeID, key, OpRead, 0, label) call. valueLen
// is always zero at this layer because the consistency check doesn't
// fetch data — the actual GetAt happens further down the stack and
// is sampled separately (Phase 2 follow-up for adapter direct reads).
func TestShardedCoordinatorObservesLeaseAndLinearizableReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "kv-sampler-read", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)
	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
	}
	rec := &recordingSampler{}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups)).WithSampler(rec)

	key := []byte("hot-key")
	_, err := coord.LinearizableReadForKey(ctx, key)
	require.NoError(t, err)
	_, err = coord.LeaseReadForKey(ctx, key)
	require.NoError(t, err)

	calls := rec.snapshot()
	require.Len(t, calls, 2, "expected one Observe per read entry")
	route, ok := engine.GetRoute(routeKey(key))
	require.True(t, ok)
	want := sampleCall{
		routeID:  route.RouteID,
		op:       keyviz.OpRead,
		label:    keyviz.LabelLegacy,
		key:      append([]byte(nil), key...),
		keyLen:   len(key),
		valueLen: 0,
	}
	require.Equal(t, want, calls[0], "linearizable read must forward the full key bytes")
	require.Equal(t, want, calls[1], "lease read must forward the full key bytes")
}

func TestShardedCoordinatorKeyVizLabelGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)
	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "kv-sampler-label-gate", NewKvFSMWithHLC(st, NewHLC()))
	t.Cleanup(stop)
	groups := map[uint64]*ShardGroup{
		1: {Engine: r, Store: st, Txn: NewLeaderProxyWithEngine(r)},
	}

	t.Run("disabled overrides to legacy", func(t *testing.T) {
		rec := &recordingSampler{}
		coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups)).WithSampler(rec)
		_, err := coord.Dispatch(ctx, &OperationGroup[OP]{
			KeyVizLabel: keyviz.LabelRedis,
			Elems:       []*Elem[OP]{{Op: Put, Key: []byte("b"), Value: []byte("v")}},
		})
		require.NoError(t, err)
		require.Equal(t, keyviz.LabelLegacy, rec.snapshot()[0].label)
	})

	t.Run("enabled preserves wrapper label", func(t *testing.T) {
		rec := &recordingSampler{}
		base := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups)).
			WithSampler(rec).
			WithKeyVizLabelsEnabled(true)
		coord := WithKeyVizLabel(base, keyviz.LabelRedis)
		ops := &OperationGroup[OP]{
			Elems: []*Elem[OP]{{Op: Put, Key: []byte("c"), Value: []byte("v")}},
		}
		_, err := coord.Dispatch(ctx, ops)
		require.NoError(t, err)
		require.Equal(t, keyviz.LabelLegacy, ops.KeyVizLabel, "wrapper must not mutate caller-owned request")

		reader, ok := coord.(interface {
			LinearizableReadForKey(context.Context, []byte) (uint64, error)
		})
		require.True(t, ok, "wrapper must preserve key-routed read capability")
		_, err = reader.LinearizableReadForKey(ctx, []byte("d"))
		require.NoError(t, err)

		calls := rec.snapshot()
		require.Len(t, calls, 2)
		require.Equal(t, keyviz.LabelRedis, calls[0].label)
		require.Equal(t, keyviz.LabelRedis, calls[1].label)
	})
}

// TestShardedCoordinatorSkipsObserveForLeadershipChecks pins the
// negative contract: leadership-only entries (IsLeaderForKey,
// VerifyLeaderForKey, RaftLeaderForKey) MUST NOT produce read
// observations — they don't represent user-facing data reads, just
// internal routing checks.
func TestShardedCoordinatorSkipsObserveForLeadershipChecks(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)

	s1 := store.NewMVCCStore()
	r1, stop1 := newSingleRaft(t, "kv-sampler-leadership", NewKvFSMWithHLC(s1, NewHLC()))
	t.Cleanup(stop1)
	groups := map[uint64]*ShardGroup{
		1: {Engine: r1, Store: s1, Txn: NewLeaderProxyWithEngine(r1)},
	}
	rec := &recordingSampler{}
	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), NewShardStore(engine, groups)).WithSampler(rec)

	key := []byte("k")
	_ = coord.IsLeaderForKey(key)
	_ = coord.VerifyLeaderForKey(context.Background(), key)
	_ = coord.RaftLeaderForKey(key)

	require.Empty(t, rec.snapshot(), "leadership checks must not produce read samples")
}
