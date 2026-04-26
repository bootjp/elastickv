package kv

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
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
	keyLen   int
	valueLen int
}

func (r *recordingSampler) Observe(routeID uint64, op keyviz.Op, keyLen, valueLen int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, sampleCall{routeID: routeID, op: op, keyLen: keyLen, valueLen: valueLen})
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
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

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
	// elem[i]. Verify each: OpWrite, exact key/value lengths, and
	// the RouteID the engine resolved for that key.
	for i, elem := range ops.Elems {
		route, ok := engine.GetRoute(elem.Key)
		require.True(t, ok)
		require.Equal(t, sampleCall{
			routeID:  route.RouteID,
			op:       keyviz.OpWrite,
			keyLen:   len(elem.Key),
			valueLen: len(elem.Value),
		}, calls[i], "Observe call %d for key %q", i, elem.Key)
	}
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
