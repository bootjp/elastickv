package kv

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// stubResolver routes any key in claim to the named group; everything
// else returns (0, false). Used by the coordinator-level resolver
// regression tests so they don't depend on the adapter package.
//
// recognisedPrefix simulates the "recognised but unresolved" path
// (codex round-2 P1 on PR #715) — keys with this prefix that miss
// claim cause RecognisesPartitionedKey to return true so the
// router fails closed instead of falling through to the engine.
type stubResolver struct {
	mu               sync.Mutex
	claim            map[string]uint64
	recognisedPrefix []byte
	calls            [][]byte
}

func (s *stubResolver) ResolveGroup(key []byte) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, append([]byte(nil), key...))
	if gid, ok := s.claim[string(key)]; ok {
		return gid, true
	}
	return 0, false
}

func (s *stubResolver) RecognisesPartitionedKey(key []byte) bool {
	if len(s.recognisedPrefix) == 0 {
		return false
	}
	return len(key) >= len(s.recognisedPrefix) &&
		string(key[:len(s.recognisedPrefix)]) == string(s.recognisedPrefix)
}

func (s *stubResolver) callKeys() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.calls))
	copy(out, s.calls)
	return out
}

// TestShardedCoordinator_DispatchHonoursPartitionResolver pins
// resolver wiring at the coordinator level: a Dispatch whose key
// is claimed by the partition resolver must land on the
// resolver's group, not the engine's default group.
//
// NOTE (round 4 review): for a single-mutation batch, this test
// passes even if groupMutations bypasses the resolver — rawLogs
// produces one request, and router.Commit's groupRequests
// re-routes by raw key. This test pins the WithPartitionResolver
// fluent wiring + raw-key dispatch, NOT the groupMutations bypass
// regression. The 2-mutation test below is the actual regression
// for groupMutations (codex P1 + gemini HIGH).
func TestShardedCoordinator_DispatchHonoursPartitionResolver(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	g1 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 1}, {CommitIndex: 1}},
	}
	g42 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 1}, {CommitIndex: 1}},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1:  {Txn: g1, Store: store.NewMVCCStore()},
		42: {Txn: g42, Store: store.NewMVCCStore()},
	}, 1, NewHLC(), nil)

	resolver := &stubResolver{claim: map[string]uint64{
		"!sqs|msg|data|p|partitioned-key": 42,
	}}
	coord.WithPartitionResolver(resolver)

	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("!sqs|msg|data|p|partitioned-key"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// The whole point: the request landed on group 42, NOT 1.
	g1.mu.Lock()
	g42.mu.Lock()
	g1Count := len(g1.requests)
	g42Count := len(g42.requests)
	g1.mu.Unlock()
	g42.mu.Unlock()

	require.Zero(t, g1Count,
		"engine's default group must NOT receive a request when the "+
			"resolver claimed the key — coordinator's groupMutations "+
			"would otherwise bypass the partition resolver")
	require.Equal(t, 1, g42Count,
		"resolver-claimed group must receive exactly one request")

	// And the resolver was indeed consulted with the raw partitioned
	// key — pins the codex-P1 fix at the coordinator-call boundary.
	calls := resolver.callKeys()
	require.NotEmpty(t, calls)
	require.Equal(t, []byte("!sqs|msg|data|p|partitioned-key"), calls[0])
}

// TestShardedCoordinator_DispatchSplitsMutationsByResolverGroup is
// the genuine regression for the Gemini-HIGH groupMutations
// bypass: a Dispatch with mutations belonging to TWO different
// partitions must split into two requests, one per group.
//
// Pre-fix path (groupMutations called c.engine.GetRoute directly):
//   - groupMutations → engine route → both mutations bundled under
//     the engine default group.
//   - rawLogs → ONE pb.Request with both mutations.
//   - router.Commit → groupRequests routes by Mutations[0].Key only
//     → request goes to group A only; group B receives nothing.
//
// Post-fix path (groupMutations consults c.router.ResolveGroup):
//   - groupMutations → resolver → mut0→A, mut1→B; grouped split.
//   - rawLogs → TWO pb.Requests, one per group.
//   - Each group receives its own request.
//
// The assertion is that BOTH groups receive a request — pre-fix,
// only one would. This is the test the round 4 review asked for.
func TestShardedCoordinator_DispatchSplitsMutationsByResolverGroup(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	g1 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 1}},
	}
	g42 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 1}},
	}
	g43 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 1}},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1:  {Txn: g1, Store: store.NewMVCCStore()},
		42: {Txn: g42, Store: store.NewMVCCStore()},
		43: {Txn: g43, Store: store.NewMVCCStore()},
	}, 1, NewHLC(), nil)

	keyP0 := []byte("!sqs|msg|data|p|partition-0-key")
	keyP1 := []byte("!sqs|msg|data|p|partition-1-key")
	coord.WithPartitionResolver(&stubResolver{claim: map[string]uint64{
		string(keyP0): 42,
		string(keyP1): 43,
	}})

	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: keyP0, Value: []byte("v0")},
			{Op: Put, Key: keyP1, Value: []byte("v1")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	g1.mu.Lock()
	g42.mu.Lock()
	g43.mu.Lock()
	defer g1.mu.Unlock()
	defer g42.mu.Unlock()
	defer g43.mu.Unlock()

	require.Zero(t, len(g1.requests),
		"engine default group must not receive a request — both "+
			"keys are partitioned-resolver claims")
	require.Equal(t, 1, len(g42.requests),
		"partition-0 group must receive exactly one request "+
			"(pre-groupMutations-fix: would receive both mutations "+
			"in one request because router.Commit routes by "+
			"Mutations[0].Key only)")
	require.Equal(t, 1, len(g43.requests),
		"partition-1 group must receive exactly one request "+
			"(pre-groupMutations-fix: would receive ZERO requests "+
			"because both mutations were bundled under the "+
			"engine-route group). This is the genuine regression "+
			"for the Gemini HIGH bypass — the fix splits "+
			"mutations across groups via c.router.ResolveGroup.")
}

// TestShardedCoordinator_DispatchFallsThroughForUnclaimedKeys pins
// the inverse: a key the resolver does NOT claim must continue to
// route via the byte-range engine. Without this guard the resolver-
// first short-circuit could mask engine routing decisions.
func TestShardedCoordinator_DispatchFallsThroughForUnclaimedKeys(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 1}},
	}
	g2 := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 2}},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1, Store: store.NewMVCCStore()},
		2: {Txn: g2, Store: store.NewMVCCStore()},
	}, 1, NewHLC(), nil)

	// Resolver only claims a key that ISN'T in the request — the
	// dispatch must fall through to the engine.
	coord.WithPartitionResolver(&stubResolver{claim: map[string]uint64{
		"!sqs|msg|data|p|other-key": 42,
	}})

	// "x" lands in [m, ∞) → engine routes to group 2.
	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("x"), Value: []byte("v")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	g1.mu.Lock()
	g2.mu.Lock()
	g1Count := len(g1.requests)
	g2Count := len(g2.requests)
	g1.mu.Unlock()
	g2.mu.Unlock()

	require.Zero(t, g1Count,
		"unclaimed key must engine-route to group 2, not group 1")
	require.Equal(t, 1, g2Count,
		"engine fallthrough must dispatch to group 2 for keys in [m, ∞)")
}
