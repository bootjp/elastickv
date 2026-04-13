package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestListDeltaCompactor_FoldsDeltas(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	server := nodes[0].redisServer
	ctx := context.Background()

	// Write base metadata and items directly (simulating existing list).
	readTS := server.readTS()
	meta := store.ListMeta{Head: 0, Tail: 2, Len: 2}
	metaBytes, err := store.MarshalListMeta(meta)
	require.NoError(t, err)
	require.NoError(t, server.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: store.ListMetaKey([]byte("mylist")), Value: metaBytes},
		{Op: kv.Put, Key: store.ListItemKey([]byte("mylist"), 0), Value: []byte("a")},
		{Op: kv.Put, Key: store.ListItemKey([]byte("mylist"), 1), Value: []byte("b")},
	}))

	// Write delta keys directly (simulating accumulated deltas).
	readTS = server.readTS()
	commitTS1 := readTS + 1
	commitTS2 := readTS + 2
	d1 := store.ListMetaDelta{HeadDelta: 0, LenDelta: 1}
	d2 := store.ListMetaDelta{HeadDelta: 0, LenDelta: 1}
	require.NoError(t, server.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: store.ListMetaDeltaKey([]byte("mylist"), commitTS1, 0), Value: store.MarshalListMetaDelta(d1)},
		{Op: kv.Put, Key: store.ListItemKey([]byte("mylist"), 2), Value: []byte("c")},
	}))
	readTS = server.readTS()
	require.NoError(t, server.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: store.ListMetaDeltaKey([]byte("mylist"), commitTS2, 0), Value: store.MarshalListMetaDelta(d2)},
		{Op: kv.Put, Key: store.ListItemKey([]byte("mylist"), 3), Value: []byte("d")},
	}))

	// Verify deltas exist.
	readTS = server.readTS()
	prefix := store.ListMetaDeltaScanPrefix([]byte("mylist"))
	deltas, err := server.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 100, readTS)
	require.NoError(t, err)
	require.Len(t, deltas, 2)

	// Run compaction with threshold 1 to force compaction.
	compactor := NewListDeltaCompactor(server.store, server.coordinator,
		WithListCompactorMaxDeltaCount(1),
	)
	err = compactor.Tick(ctx)
	require.NoError(t, err)

	// After compaction: base metadata should have merged values.
	readTS = server.readTS()
	val, err := server.store.GetAt(ctx, store.ListMetaKey([]byte("mylist")), readTS)
	require.NoError(t, err)
	compactedMeta, err := store.UnmarshalListMeta(val)
	require.NoError(t, err)
	require.Equal(t, int64(4), compactedMeta.Len) // 2 + 1 + 1

	// Deltas should be deleted after compaction.
	deltas, err = server.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), 100, readTS)
	require.NoError(t, err)
	require.Empty(t, deltas)
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
