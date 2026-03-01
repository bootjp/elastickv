package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

type stubAdapterCoordinator struct {
	clock *kv.HLC
}

func (s *stubAdapterCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return &kv.CoordinateResponse{}, nil
}

func (s *stubAdapterCoordinator) IsLeader() bool {
	return true
}

func (s *stubAdapterCoordinator) VerifyLeader() error {
	return nil
}

func (s *stubAdapterCoordinator) RaftLeader() raft.ServerAddress {
	return ""
}

func (s *stubAdapterCoordinator) IsLeaderForKey([]byte) bool {
	return true
}

func (s *stubAdapterCoordinator) VerifyLeaderForKey([]byte) error {
	return nil
}

func (s *stubAdapterCoordinator) RaftLeaderForKey([]byte) raft.ServerAddress {
	return ""
}

func (s *stubAdapterCoordinator) Clock() *kv.HLC {
	if s.clock == nil {
		s.clock = kv.NewHLC()
	}
	return s.clock
}

type tsTrackingStore struct {
	store.MVCCStore
	scanTS []uint64
}

func (s *tsTrackingStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.scanTS = append(s.scanTS, ts)
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestPatternScanBounds(t *testing.T) {
	t.Parallel()

	start, end := patternScanBounds([]byte("*"))
	require.Nil(t, start)
	require.Nil(t, end)

	start, end = patternScanBounds([]byte("test*"))
	require.Equal(t, []byte("test"), start)
	require.Equal(t, prefixScanEnd([]byte("test")), end)

	start, end = patternScanBounds([]byte("*test"))
	require.Nil(t, start)
	require.Nil(t, end)

	start, end = patternScanBounds([]byte("ab*cd"))
	require.Equal(t, []byte("ab"), start)
	require.Equal(t, prefixScanEnd([]byte("ab")), end)
}

func TestListPatternScanBounds(t *testing.T) {
	t.Parallel()

	start, end := listPatternScanBounds(store.ListMetaPrefix, []byte("test*"))
	require.Equal(t, []byte(store.ListMetaPrefix+"test"), start)
	require.Equal(t, []byte(store.ListMetaPrefix+"tesu"), end)
}

func TestMatchesAsteriskPattern(t *testing.T) {
	t.Parallel()

	cases := []struct {
		pattern []byte
		key     []byte
		match   bool
	}{
		{[]byte("*"), []byte("any"), true},
		{[]byte("test*"), []byte("test-key"), true},
		{[]byte("test*"), []byte("toast"), false},
		{[]byte("*suffix"), []byte("with-suffix"), true},
		{[]byte("pre*mid*suf"), []byte("pre-1-mid-2-suf"), true},
		{[]byte("pre*mid*suf"), []byte("pre-suf"), false},
	}

	for _, tc := range cases {
		require.Equal(t, tc.match, matchesAsteriskPattern(tc.pattern, tc.key))
	}
}

func TestCollectUserKeys_FiltersByPattern(t *testing.T) {
	t.Parallel()

	r := &RedisServer{}
	kvs := []*store.KVPair{
		{Key: []byte("test:key"), Value: []byte("v")},
		{Key: []byte("toast:key"), Value: []byte("v")},
		{Key: store.ListMetaKey([]byte("test:list")), Value: []byte("m")},
		{Key: store.ListItemKey([]byte("test:list"), 1), Value: []byte("i")},
	}

	keys := r.collectUserKeys(kvs, []byte("test*"))
	require.Len(t, keys, 2)
	require.Contains(t, keys, "test:key")
	require.Contains(t, keys, "test:list")
}

func TestLocalKeysPattern_FindsListKeysForUserPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("test:key"), []byte("v"), 1, 0))
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey([]byte("test:list")), []byte("m"), 2, 0))
	require.NoError(t, st.PutAt(ctx, store.ListItemKey([]byte("test:list"), 1), []byte("i"), 3, 0))
	require.NoError(t, st.PutAt(ctx, []byte("toast:key"), []byte("v"), 4, 0))

	r := &RedisServer{
		store:       st,
		coordinator: &stubAdapterCoordinator{clock: kv.NewHLC()},
	}

	keys, err := r.localKeysPattern([]byte("test*"))
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.Contains(t, keys, []byte("test:key"))
	require.Contains(t, keys, []byte("test:list"))
}

func TestLocalKeysPattern_UsesSingleSnapshotTSAcrossScans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := store.NewMVCCStore()
	require.NoError(t, base.PutAt(ctx, []byte("test:key"), []byte("v"), 1, 0))
	require.NoError(t, base.PutAt(ctx, store.ListMetaKey([]byte("test:list")), []byte("m"), 2, 0))
	require.NoError(t, base.PutAt(ctx, store.ListItemKey([]byte("test:list"), 1), []byte("i"), 3, 0))

	tracking := &tsTrackingStore{MVCCStore: base}
	r := &RedisServer{
		store:       tracking,
		coordinator: &stubAdapterCoordinator{clock: kv.NewHLC()},
	}

	_, err := r.localKeysPattern([]byte("test*"))
	require.NoError(t, err)
	require.Len(t, tracking.scanTS, 3)
	for _, ts := range tracking.scanTS {
		require.Equal(t, tracking.scanTS[0], ts)
	}
}

func TestLocalKeysExact_FindsListByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	meta, err := store.MarshalListMeta(store.ListMeta{Head: 1, Tail: 2, Len: 1})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey([]byte("exact:list")), meta, 1, 0))
	require.NoError(t, st.PutAt(ctx, store.ListItemKey([]byte("exact:list"), 1), []byte("v"), 2, 0))

	r := &RedisServer{
		store:       st,
		coordinator: &stubAdapterCoordinator{clock: kv.NewHLC()},
	}

	keys, err := r.localKeysExact([]byte("exact:list"))
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, []byte("exact:list"), keys[0])
}
