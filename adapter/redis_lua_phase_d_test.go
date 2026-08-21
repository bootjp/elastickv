package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestRedisLuaBeginReadTimestampPhaseDNormalizesEmptySnapshotSentinel(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	allocator := &distributionTSOAllocator{base: 100, phaseD: true, phaseDFloor: 10}
	coord := newDistributionCoordinatorStub(st, true)
	coord.allocator = allocator
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	scriptCtx, err := newLuaScriptContext(context.Background(), server)
	require.NoError(t, err)
	defer scriptCtx.Close()

	require.Equal(t, uint64(1), scriptCtx.startTS)
	require.Zero(t, allocator.count, "an empty Redis Lua snapshot must not allocate ahead of Raft apply")
}
