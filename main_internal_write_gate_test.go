package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

// A sharded coordinator owns the route table, so Internal.Forward must receive
// its route-floor gate: follower-forwarded raw writes are stamped on the leader
// and would otherwise skip the MinWriteTSExclusive check entirely.
func TestInternalTimestampOptions_WiresShardedWriteGate(t *testing.T) {
	t.Parallel()

	var sharded kv.Coordinator = &kv.ShardedCoordinator{}
	_, isGate := sharded.(kv.MutationWriteGate)
	require.True(t, isGate, "ShardedCoordinator must satisfy kv.MutationWriteGate")
	require.NotEmpty(t, internalTimestampOptions(sharded))
}

// The Redis read fence resolves a target's group through
// kv.GroupLeaderRoutableCoordinator. main.go hands the adapters a
// startupGatedCoordinator, not the ShardedCoordinator, so if the wrapper does
// not forward the group-keyed methods the assertion fails at runtime and every
// fence target silently falls back to key resolution -- re-introducing the
// legacy-group collapse that kv.ReadFenceTarget exists to prevent. Unit tests
// that use an unwrapped coordinator cannot catch that, so it is pinned here.
func TestStartupGatedCoordinatorForwardsGroupRouting(t *testing.T) {
	t.Parallel()

	var c any = startupGatedCoordinator{}
	if _, ok := c.(kv.GroupLeaderRoutableCoordinator); !ok {
		t.Fatal("startupGatedCoordinator must satisfy kv.GroupLeaderRoutableCoordinator")
	}
	if _, ok := c.(interface {
		LeaseReadForGroup(context.Context, uint64) (uint64, error)
	}); !ok {
		t.Fatal("startupGatedCoordinator must forward LeaseReadForGroup")
	}
	if _, ok := c.(kv.GroupRoutableCoordinator); !ok {
		t.Fatal("startupGatedCoordinator must satisfy kv.GroupRoutableCoordinator")
	}
}

// writeGateStubCoordinator stands in for the ShardedCoordinator underneath the
// startup wrapper. The embedded interface is nil on purpose: only the two
// optional methods are exercised here.
type writeGateStubCoordinator struct {
	kv.Coordinator

	gateCalls  int
	gateMuts   []*pb.Mutation
	gateTS     uint64
	gateErr    error
	observed   [][]*pb.Request
	observeHit int
}

func (s *writeGateStubCoordinator) EnsureMutationsWriteAllowed(muts []*pb.Mutation, commitTS uint64) error {
	s.gateCalls++
	s.gateMuts = muts
	s.gateTS = commitTS
	return s.gateErr
}

func (s *writeGateStubCoordinator) ObserveForwardedRequests(reqs []*pb.Request) {
	s.observeHit++
	s.observed = append(s.observed, reqs)
}

// main.go hands the adapters a startupGatedCoordinator, so internalTimestampOptions
// probes the wrapper -- not the ShardedCoordinator it holds. If the wrapper does
// not forward the route-floor gate and the forwarded-write observer, both options
// are silently dropped in production: follower-forwarded raw and transactional
// commits skip the MinWriteTSExclusive check entirely and forwarded-write
// sampling goes dark. TestInternalTimestampOptions_WiresShardedWriteGate uses an
// unwrapped coordinator and cannot catch that, so it is pinned here.
func TestStartupGatedCoordinatorForwardsWriteGateAndObserver(t *testing.T) {
	t.Parallel()

	inner := &writeGateStubCoordinator{}
	wrapped := startupGatedCoordinator{inner: inner}

	gate, ok := any(wrapped).(kv.MutationWriteGate)
	require.True(t, ok, "startupGatedCoordinator must satisfy kv.MutationWriteGate")
	observer, ok := any(wrapped).(interface{ ObserveForwardedRequests([]*pb.Request) })
	require.True(t, ok, "startupGatedCoordinator must forward ObserveForwardedRequests")

	muts := []*pb.Mutation{{Key: []byte("k")}}
	require.NoError(t, gate.EnsureMutationsWriteAllowed(muts, 42))
	require.Equal(t, 1, inner.gateCalls, "the gate must reach the wrapped coordinator")
	require.Equal(t, muts, inner.gateMuts)
	require.Equal(t, uint64(42), inner.gateTS)

	reqs := []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	observer.ObserveForwardedRequests(reqs)
	require.Equal(t, 1, inner.observeHit, "sampling must reach the wrapped coordinator")
	require.Equal(t, [][]*pb.Request{reqs}, inner.observed)

	// internalTimestampOptions is what actually installs them, and it is given
	// the wrapper in the production path.
	require.NotEmpty(t, internalTimestampOptions(wrapped))
}

// A coordinator with no route table underneath must keep the permissive
// behaviour that leaving the gate unset had, rather than failing writes closed.
func TestStartupGatedCoordinatorWriteGateAllowsWithoutRouteTable(t *testing.T) {
	t.Parallel()

	wrapped := startupGatedCoordinator{inner: struct{ kv.Coordinator }{}}
	gate, ok := any(wrapped).(kv.MutationWriteGate)
	require.True(t, ok)
	require.NoError(t, gate.EnsureMutationsWriteAllowed([]*pb.Mutation{{Key: []byte("k")}}, 1))

	observer, ok := any(wrapped).(interface{ ObserveForwardedRequests([]*pb.Request) })
	require.True(t, ok)
	reqs := []*pb.Request{{Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}}
	require.NotPanics(t, func() { observer.ObserveForwardedRequests(reqs) })
}
