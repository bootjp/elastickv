package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func newPartitionTxnTestStore(t *testing.T) (*ShardStore, []byte) {
	t.Helper()
	engine := distribution.NewEngine()
	engine.UpdateRoute(nil, nil, 1)
	groups := make(map[uint64]*ShardGroup)
	for _, id := range []uint64{1, 2} {
		st := store.NewMVCCStore()
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		groups[id] = &ShardGroup{Store: st}
	}
	key := []byte("partition|primary")
	resolver := &fakePartitionResolver{
		routes: map[string]uint64{string(key): 2}, recognisedPrefix: []byte("partition|"),
	}
	return NewShardStore(engine, groups).WithPartitionResolver(resolver), key
}

func TestShardStorePartitionTxnRecordsFollowEmbeddedOwner(t *testing.T) {
	s, primary := newPartitionTxnTestStore(t)
	ctx := t.Context()
	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"lock", txnLockKey(primary)},
		{"intent", txnIntentKey(primary)},
		{"meta", append(append([]byte(nil), txnMetaPrefixBytes...), primary...)},
		{"commit", txnCommitKey(primary, 10)},
		{"rollback", txnRollbackKey(primary, 10)},
		{"success", TxnSuccessMarkerKey(primary, 10, 20, primary)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, s.groups[1].Store.PutAt(ctx, tc.key, []byte("wrong-catalog-group"), 30, 0))
			require.NoError(t, s.groups[2].Store.PutAt(ctx, tc.key, []byte("partition-owner"), 20, 0))
			value, err := s.GetAt(ctx, tc.key, ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, []byte("partition-owner"), value)
			value, err = s.GetGroupAt(ctx, 2, tc.key, ^uint64(0))
			require.NoError(t, err, "the same explicit-group check is used after a follower forwards metadata reads")
			require.Equal(t, []byte("partition-owner"), value)
			_, err = s.GetAtWithReadFence(ctx, tc.key, ^uint64(0), 1, 0)
			require.ErrorIs(t, err, ErrExplicitGroupRouteOwnerMismatch)
			_, err = s.GetGroupAt(ctx, 1, tc.key, ^uint64(0))
			require.ErrorIs(t, err, ErrExplicitGroupRouteOwnerMismatch)
			require.NoError(t, s.groups[2].Store.DeleteAt(ctx, tc.key, 40))
			_, err = s.GetAt(ctx, tc.key, ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound, "a missing owner record cannot fall back to a catalog-group impostor")
		})
	}
	// Ordinary calls keep the documented byte-range contract; partition data
	// readers already use GetGroupAt, unlike resolver-driven transaction records.
	require.NoError(t, s.groups[1].Store.PutAt(ctx, primary, []byte("ordinary-catalog-read"), 50, 0))
	value, err := s.GetAt(ctx, primary, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("ordinary-catalog-read"), value)
}

func TestShardStorePartitionTxnOwnerFailureIsNotAbsent(t *testing.T) {
	for _, mode := range []string{"unresolved", "zero", "missing-group", "missing-store"} {
		t.Run(mode, func(t *testing.T) {
			s, primary := newPartitionTxnTestStore(t)
			want := ErrInvalidRequest
			switch mode {
			case "unresolved":
				primary = []byte("partition|unresolved")
			case "zero":
				s.WithPartitionResolver(&fakePartitionResolver{routes: map[string]uint64{string(primary): 0}, recognisedPrefix: []byte("partition|")})
			case "missing-group":
				delete(s.groups, 2)
				want = ErrLeaderNotFound
			case "missing-store":
				s.groups[2].Store = nil
				want = ErrLeaderNotFound
			}
			_, err := s.GetAt(t.Context(), txnCommitKey(primary, 10), ^uint64(0))
			require.ErrorIs(t, err, want)
			require.NotErrorIs(t, err, store.ErrKeyNotFound)
			_, _, err = s.groupForTxnPrimary(primary)
			require.ErrorIs(t, err, want)
		})
	}
}

func TestShardStorePartitionTxnExpiredAbortUsesOwner(t *testing.T) {
	s, primary := newPartitionTxnTestStore(t)
	for _, group := range s.groups {
		fsm, ok := NewKvFSMWithHLC(group.Store, NewHLC()).(*kvFSM)
		require.True(t, ok)
		group.Txn = &localApplyTransactional{fsm: fsm}
	}
	aborted, err := s.tryAbortExpiredPrimary(t.Context(), primary, 10)
	require.NoError(t, err)
	require.True(t, aborted)
	_, err = s.groups[2].Store.GetAt(t.Context(), txnRollbackKey(primary, 10), ^uint64(0))
	require.NoError(t, err)
	_, err = s.groups[1].Store.GetAt(t.Context(), txnRollbackKey(primary, 10), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStorePartitionTxnRetainsCatalogStagedGuard(t *testing.T) {
	s, primary := newPartitionTxnTestStore(t)
	key := txnCommitKey(primary, 10)
	require.NoError(t, s.groups[2].Store.PutAt(t.Context(), key, encodeTxnCommitRecord(20), 20, 0))
	require.NoError(t, s.engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1, Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1,
			State: distribution.RouteStateActive, StagedVisibilityActive: true, MigrationJobID: 7}},
	}))
	_, err := s.GetAt(t.Context(), key, ^uint64(0))
	require.ErrorIs(t, err, ErrExplicitGroupStagedVisibilityUnresolved,
		"resolver ownership must not bypass the existing cross-group catalog migration guard")
}
