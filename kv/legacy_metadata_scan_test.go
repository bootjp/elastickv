package kv

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type legacyMetadataPresenceStore struct {
	store.MVCCStore
	checks atomic.Int64
}

func (s *legacyMetadataPresenceStore) VersionExistsAtOrBefore(ctx context.Context, key []byte, ts uint64) (bool, error) {
	s.checks.Add(1)
	reader, ok := s.MVCCStore.(store.VersionPresenceReader)
	if !ok {
		return false, fmt.Errorf("test store has no version-presence reader")
	}
	return reader.VersionExistsAtOrBefore(ctx, key, ts)
}

func TestScanGroupOwnedPageAt_ClassifiesLegacyMetadata(t *testing.T) {
	t.Parallel()
	families := []struct {
		name string
		key  func([]byte) []byte
	}{
		{"hash", store.HashMetaKey},
		{"set", store.SetMetaKey},
		{"zset", store.ZSetMetaKey},
		{"stream", store.StreamMetaKey},
	}
	cases := []struct {
		name      string
		ownerRow  bool
		tombstone bool
		remote    bool
		staged    uint64
		want      LegacyMetadataRowReason
		checks    int64
	}{
		{name: "served", want: LegacyMetadataServed, checks: 1},
		{name: "shadowed", ownerRow: true, want: LegacyMetadataOwnerHasVersion, checks: 1},
		{name: "tombstone", tombstone: true, want: LegacyMetadataOwnerHasVersion, checks: 1},
		{name: "remote owner", remote: true, want: LegacyMetadataOwnerLedElsewhere},
		{name: "staged source", staged: 1, want: LegacyMetadataStaged},
		{name: "staged owner", staged: 2, want: LegacyMetadataStaged},
	}
	for _, family := range families {
		for _, tc := range cases {
			t.Run(family.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				st, groups := newSplitRedisShardStoreForGroupScan()
				owner := &legacyMetadataPresenceStore{MVCCStore: groups[2].Store}
				groups[2].Store = owner
				if tc.remote {
					groups[2].Engine = &followerProxyEngine{}
				}
				if tc.staged != 0 {
					routes := []distribution.RouteDescriptor{
						{RouteID: 1, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
						{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
					}
					routes[tc.staged-1].StagedVisibilityActive = true
					routes[tc.staged-1].MigrationJobID = 9
					require.NoError(t, st.engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 10, Routes: routes}))
				}
				key := family.key([]byte("zebra"))
				require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("legacy"), 5, 0))
				if tc.ownerRow {
					require.NoError(t, owner.PutAt(ctx, key, []byte("current"), 6, 0))
				}
				if tc.tombstone {
					require.NoError(t, owner.DeleteAt(ctx, key, 6))
				}
				page, err := st.ScanGroupOwnedPageAt(ctx, 1, key, nil, 10, 10)
				require.NoError(t, err)
				require.Equal(t, st.ReadRouteVersion(), page.CatalogVersion)
				require.Equal(t, []LegacyMetadataRow{{Row: &store.KVPair{Key: key, Value: []byte("legacy")}, OwnerGroupID: 2, Reason: tc.want}}, page.LegacyRows)
				require.Equal(t, tc.checks, owner.checks.Load())
				wantServed := []*store.KVPair{}
				if tc.want == LegacyMetadataServed {
					wantServed = append(wantServed, &store.KVPair{Key: key, Value: []byte("legacy")})
				}
				require.Equal(t, wantServed, page.Rows)
				require.Nil(t, page.Resume)
				previous, _, err := st.ScanGroupOwnedAt(ctx, 1, key, nil, 10, 10)
				require.NoError(t, err)
				require.Equal(t, previous, page.Rows)
			})
		}
	}
}

func TestScanGroupOwnedPageAt_ExcludesCanonicalAndDeltaRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, groups := newSplitRedisShardStoreForGroupScan()
	keys := [][]byte{
		store.HashMetaKey([]byte("apple")),
		store.SetMetaKey([]byte("apple")),
		store.ZSetMetaKey([]byte("apple")),
		store.StreamMetaKey([]byte("apple")),
		store.HashMetaDeltaKey([]byte("zebra"), 5, 0),
		store.SetMetaDeltaKey([]byte("zebra"), 5, 0),
		store.ZSetMetaDeltaKey([]byte("zebra"), 5, 0),
		store.ListMetaKey([]byte("zebra")),
	}
	for _, key := range keys {
		require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("value"), 5, 0))
	}
	page, err := st.ScanGroupOwnedPageAt(ctx, 1, nil, nil, 20, 10)
	require.NoError(t, err)
	require.Empty(t, page.LegacyRows)
	previous, _, err := st.ScanGroupOwnedAt(ctx, 1, nil, nil, 20, 10)
	require.NoError(t, err)
	require.Equal(t, previous, page.Rows)
}

func TestScanGroupOwnedPageAt_ResumesAfterFilteredPhysicalPage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, groups := newSplitRedisShardStoreForGroupScan()
	source := &rowCountingMVCCStore{MVCCStore: groups[1].Store}
	groups[1].Store = source
	groups[2].Engine = &followerProxyEngine{}
	prefix := []byte(store.HashMetaPrefix)
	for i := range 5 {
		require.NoError(t, source.PutAt(ctx, store.HashMetaKey([]byte(fmt.Sprintf("zebra%d", i))), []byte("legacy"), 5, 0))
	}
	var seen int
	start := prefix
	for range 3 {
		page, err := st.ScanGroupOwnedPageAt(ctx, 1, start, prefixScanEnd(prefix), 2, 10)
		require.NoError(t, err)
		require.Empty(t, page.Rows)
		seen += len(page.LegacyRows)
		if page.Resume == nil {
			break
		}
		start = append(append([]byte(nil), page.Resume...), 0)
	}
	require.Equal(t, 5, seen)
	require.Equal(t, 5, source.rowsRead())
}

type legacyMetadataChangingStore struct {
	store.MVCCStore
	afterScan func()
}

func (s *legacyMetadataChangingStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	rows, err := s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	s.afterScan()
	return rows, err
}

func TestScanGroupOwnedPageAt_RejectsUnknownOrChangedEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("catalog changes", func(t *testing.T) {
		st, groups := newSplitRedisShardStoreForGroupScan()
		groups[1].Store = &legacyMetadataChangingStore{MVCCStore: groups[1].Store, afterScan: func() {
			require.NoError(t, st.engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 10, Routes: []distribution.RouteDescriptor{
				{RouteID: 1, End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
				{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
			}}))
		}}
		page, err := st.ScanGroupOwnedPageAt(ctx, 1, nil, nil, 10, 10)
		require.ErrorIs(t, err, ErrReadRouteVersionUnavailable)
		require.Equal(t, GroupOwnedScanPage{}, page)
	})
	t.Run("not source leader", func(t *testing.T) {
		st, groups := newSplitRedisShardStoreForGroupScan()
		groups[1].Engine = &followerProxyEngine{}
		page, err := st.ScanGroupOwnedPageAt(ctx, 1, nil, nil, 10, 10)
		require.ErrorIs(t, err, raftengine.ErrNotLeader)
		require.Equal(t, GroupOwnedScanPage{}, page)
	})
	t.Run("source leadership lost during page", func(t *testing.T) {
		st, groups := newSplitRedisShardStoreForGroupScan()
		groups[1].Store = &legacyMetadataChangingStore{MVCCStore: groups[1].Store, afterScan: func() {
			groups[1].Engine = &followerProxyEngine{}
		}}
		page, err := st.ScanGroupOwnedPageAt(ctx, 1, nil, nil, 10, 10)
		require.ErrorIs(t, err, raftengine.ErrNotLeader)
		require.Equal(t, GroupOwnedScanPage{}, page)
	})
	t.Run("owner lacks local presence reader", func(t *testing.T) {
		st, groups := newSplitRedisShardStoreForGroupScan()
		// Embedding the base interface deliberately hides optional presence support.
		groups[2].Store = &struct{ store.MVCCStore }{groups[2].Store}
		key := store.HashMetaKey([]byte("zebra"))
		require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("legacy"), 5, 0))
		page, err := st.ScanGroupOwnedPageAt(ctx, 1, nil, nil, 10, 10)
		require.ErrorContains(t, err, "cannot answer local version presence")
		require.Equal(t, GroupOwnedScanPage{}, page)
	})
	t.Run("missing source", func(t *testing.T) {
		st, _ := newSplitRedisShardStoreForGroupScan()
		page, err := st.ScanGroupOwnedPageAt(ctx, 99, nil, nil, 10, 10)
		require.ErrorIs(t, err, ErrLeaderNotFound)
		require.Equal(t, GroupOwnedScanPage{}, page)
	})
	for _, limit := range []int{-1, 0, 513} {
		t.Run(fmt.Sprintf("invalid limit %d", limit), func(t *testing.T) {
			st, _ := newSplitRedisShardStoreForGroupScan()
			page, err := st.ScanGroupOwnedPageAt(ctx, 1, nil, nil, limit, 10)
			require.Error(t, err)
			require.Equal(t, GroupOwnedScanPage{}, page)
		})
	}
}
