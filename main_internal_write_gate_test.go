package main

import (
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
