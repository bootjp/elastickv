package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
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
