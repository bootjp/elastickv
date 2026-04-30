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
type stubResolver struct {
	mu    sync.Mutex
	claim map[string]uint64
	calls [][]byte
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

func (s *stubResolver) callKeys() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.calls))
	copy(out, s.calls)
	return out
}

// TestShardedCoordinator_DispatchHonoursPartitionResolver pins the
// Gemini-HIGH fix: ShardedCoordinator's groupMutations path now
// calls c.router.ResolveGroup, so a Dispatch whose key is claimed
// by the partition resolver MUST land on the resolver's group, not
// the engine's default group. Before the fix the coordinator
// bypassed the resolver entirely and partitioned-FIFO traffic
// silently mis-routed through 2PC.
//
// Two-group setup: engine routes everything to group 1; resolver
// claims one specific key for group 42. Dispatch on that key must
// hit group 42's recordingTransactional, leaving group 1's
// recorder empty.
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
