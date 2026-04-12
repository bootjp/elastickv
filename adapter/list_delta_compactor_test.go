package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestListDeltaCompactor_FoldsDeltas(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	server := nodes[0].redisServer

	// Push items to create delta keys (standalone RPUSH uses delta pattern).
	ctx := context.Background()
	_, err := server.listRPush(ctx, []byte("mylist"), [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	_, err = server.listRPush(ctx, []byte("mylist"), [][]byte{[]byte("c")})
	require.NoError(t, err)

	// Verify deltas exist.
	readTS := server.readTS()
	prefix := store.ListMetaDeltaScanPrefix([]byte("mylist"))
	deltas, err := server.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 100, readTS)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(deltas), 2, "should have at least 2 delta keys")

	// Verify base metadata does not exist (only deltas).
	_, err = server.store.GetAt(ctx, store.ListMetaKey([]byte("mylist")), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	// Run compaction with threshold 1 to force compaction.
	compactor := NewListDeltaCompactor(server.store, server.coordinator,
		WithListCompactorMaxDeltaCount(1),
	)
	err = compactor.Tick(ctx)
	require.NoError(t, err)

	// After compaction: base metadata should exist with correct values.
	readTS = server.readTS()
	val, err := server.store.GetAt(ctx, store.ListMetaKey([]byte("mylist")), readTS)
	require.NoError(t, err)
	meta, err := store.UnmarshalListMeta(val)
	require.NoError(t, err)
	require.Equal(t, int64(3), meta.Len)
	require.Equal(t, int64(0), meta.Head)

	// Deltas should be deleted after compaction.
	deltas, err = server.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 100, readTS)
	require.NoError(t, err)
	require.Empty(t, deltas, "all deltas should be deleted after compaction")

	// Data should still be readable via resolveListMeta.
	resolvedMeta, exists, err := server.resolveListMeta(ctx, []byte("mylist"), readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(3), resolvedMeta.Len)
}

func TestGroupDeltasByUserKey(t *testing.T) {
	t.Parallel()

	entries := []*store.KVPair{
		{Key: store.ListMetaDeltaKey([]byte("a"), 100, 0)},
		{Key: store.ListMetaDeltaKey([]byte("a"), 200, 0)},
		{Key: store.ListMetaDeltaKey([]byte("b"), 100, 0)},
	}
	groups := groupDeltasByUserKey(entries)
	require.Len(t, groups, 2)
	require.Len(t, groups["a"], 2)
	require.Len(t, groups["b"], 1)
}
