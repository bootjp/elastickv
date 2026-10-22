package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestPrimaryCommitEvidenceDistinguishesMissingFromUnavailable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		prepare   func(*ShardGroup)
		want      PrimaryCommitEvidence
		wantError bool
	}{
		{name: "missing local record", want: PrimaryCommitEvidence{Readable: true}},
		{name: "committed", prepare: func(g *ShardGroup) {
			require.NoError(t, g.Store.PutAt(ctx, txnCommitKey([]byte("key"), 50), encodeTxnCommitRecord(60), 60, 0))
		}, want: PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}},
		{name: "rolled back", prepare: func(g *ShardGroup) {
			require.NoError(t, g.Store.PutAt(ctx, txnRollbackKey([]byte("key"), 50), encodeTxnRollbackRecord(), 60, 0))
		}, want: PrimaryCommitEvidence{Readable: true}},
		{name: "remote primary", prepare: func(g *ShardGroup) { g.Store = nil }},
		{name: "follower primary", prepare: func(g *ShardGroup) { g.Engine = &followerProxyEngine{leader: "127.0.0.1:1"} }},
		{name: "corrupt record", prepare: func(g *ShardGroup) {
			require.NoError(t, g.Store.PutAt(ctx, txnCommitKey([]byte("key"), 50), []byte("bad"), 60, 0))
		}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}},
			}))
			group := &ShardGroup{Store: store.NewMVCCStore()}
			if test.prepare != nil {
				test.prepare(group)
			}
			shards := NewShardStore(engine, map[uint64]*ShardGroup{1: group})
			evidence, err := shards.PrimaryCommitEvidence(ctx, []byte("key"), 50)
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, evidence)
		})
	}
}

func TestPrimaryCommitEvidenceDoesNotReadStagedOwnerAsAbsent(t *testing.T) {
	t.Parallel()
	shards, _ := newStagedVisibilityShardStore(t)
	evidence, err := shards.PrimaryCommitEvidence(context.Background(), []byte("key"), 50)
	require.NoError(t, err)
	require.False(t, evidence.Readable)
}

func TestPrimaryCommitEvidenceSurvivesStoreReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, txnCommitKey([]byte("key"), 50), encodeTxnCommitRecord(60), 60, 0))
	require.NoError(t, st.Close())
	reopened, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}},
	}))
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: reopened}})
	evidence, err := shards.PrimaryCommitEvidence(ctx, []byte("key"), 50)
	require.NoError(t, err)
	require.Equal(t, PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}, evidence)
}

func TestPrimaryCommitEvidenceUsesPartitionOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	key := []byte("partition:primary")
	for _, resolved := range []bool{false, true} {
		t.Run(map[bool]string{false: "unresolved", true: "resolved"}[resolved], func(t *testing.T) {
			engine := distribution.NewEngine()
			engine.UpdateRoute(nil, nil, 1)
			owner := store.NewMVCCStore()
			require.NoError(t, owner.PutAt(ctx, txnCommitKey(key, 50), encodeTxnCommitRecord(60), 60, 0))
			resolver := &fakePartitionResolver{routes: map[string]uint64{}, recognisedPrefix: []byte("partition:")}
			if resolved {
				resolver.routes[string(key)] = 2
			}
			shards := NewShardStore(engine, map[uint64]*ShardGroup{
				1: {Store: store.NewMVCCStore()}, 2: {Store: owner},
			}).WithPartitionResolver(resolver)
			evidence, err := shards.PrimaryCommitEvidence(ctx, key, 50)
			require.NoError(t, err)
			if resolved {
				require.Equal(t, PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}, evidence)
			} else {
				require.False(t, evidence.Readable)
			}
		})
	}
}

type primaryEvidenceReadHookStore struct {
	store.MVCCStore
	beforeRead func()
}

func (s *primaryEvidenceReadHookStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.beforeRead()
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func TestPrimaryCommitEvidenceDiscardsChangedOwnership(t *testing.T) {
	t.Parallel()
	key := []byte("partition:primary")
	for _, change := range []string{"catalog", "partition map"} {
		t.Run(change, func(t *testing.T) {
			engine := distribution.NewEngine()
			engine.UpdateRoute(nil, nil, 1)
			resolver := &fakePartitionResolver{routes: map[string]uint64{string(key): 1}, recognisedPrefix: []byte("partition:")}
			st := &primaryEvidenceReadHookStore{MVCCStore: store.NewMVCCStore(), beforeRead: func() {
				if change == "catalog" {
					require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
						Version: 2, Routes: []distribution.RouteDescriptor{{RouteID: 2, GroupID: 2, State: distribution.RouteStateActive}},
					}))
				} else {
					resolver.routes[string(key)] = 2
				}
			}}
			shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: st}, 2: {Store: store.NewMVCCStore()}}).WithPartitionResolver(resolver)
			evidence, err := shards.PrimaryCommitEvidence(context.Background(), key, 50)
			require.NoError(t, err)
			require.False(t, evidence.Readable)
		})
	}
}
