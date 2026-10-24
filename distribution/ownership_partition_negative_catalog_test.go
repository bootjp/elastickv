package distribution

import (
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type negativePartitionScanStore struct {
	store.MVCCStore
	limit, calls int
}

func (s *negativePartitionScanStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.limit, s.calls = limit, s.calls+1
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func putNegativePartitionManifest(t *testing.T, st store.MVCCStore, m OwnershipPartitionManifest, ts uint64) []byte {
	t.Helper()
	raw, err := EncodeOwnershipPartitionManifest(m)
	require.NoError(t, err)
	hash, err := OwnershipPartitionDescriptorHash(m.Parent)
	require.NoError(t, err)
	key, err := CatalogOwnershipPartitionKey(m.Parent.RouteID, hash)
	require.NoError(t, err)
	require.NoError(t, st.ApplyMutations(t.Context(), []*store.KVPairMutation{{Op: store.OpTypePut, Key: key, Value: raw},
		{Op: store.OpTypePut, Key: CatalogVersionKey(), Value: EncodeCatalogVersion(m.Version)}}, nil, ts-1, ts))
	return key
}

func TestOwnershipPartitionNegativeLookupPinnedExactAndBounded(t *testing.T) {
	st := &negativePartitionScanStore{MVCCStore: store.NewMVCCStore()}
	catalog := NewCatalogStore(st)
	m := ownershipPartitionManifestFixture()
	putNegativePartitionManifest(t, st, m, 20)
	_, found, err := catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, m.Parent.Start, m.Parent.End, 19)
	require.NoError(t, err)
	require.False(t, found)
	actual, found, err := catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, m.Parent.Start, m.Parent.End, 20)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, m, actual)
	require.Equal(t, 2, st.limit, "never enumerate the immutable ledger without a bound")
	_, found, err = catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, []byte("n"), m.Parent.End, 20)
	require.NoError(t, err)
	require.False(t, found, "containment is not exact prior geometry")
	_, found, err = catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID+1, m.Parent.Start, m.Parent.End, 20)
	require.NoError(t, err)
	require.False(t, found)
}

func TestOwnershipPartitionNegativeLookupRejectsAmbiguityAndUncommittedC(t *testing.T) {
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	m := ownershipPartitionManifestFixture()
	putNegativePartitionManifest(t, st, m, 20)
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(m.PreviousVersion), 21, 0))
	_, _, err := catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, m.Parent.Start, m.Parent.End, 21)
	require.ErrorIs(t, err, ErrOwnershipPartitionManifest)
	other := ownershipPartitionManifestFixture()
	other.Parent.GroupID = 3
	for i := range other.Children {
		other.Children[i].GroupID = 3
	}
	putNegativePartitionManifest(t, st, other, 30)
	_, _, err = catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, m.Parent.Start, m.Parent.End, math.MaxUint64)
	require.ErrorIs(t, err, ErrOwnershipPartitionManifest, "two identities for a route cannot be guessed by owner or version")
	_, found, err := catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, m.Parent.Start, m.Parent.End, 20)
	require.NoError(t, err)
	require.True(t, found, "later ambiguity must not contaminate a pinned earlier read")
}

func TestOwnershipPartitionNegativeLookupRejectsCorruptKeyIdentity(t *testing.T) {
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	m := ownershipPartitionManifestFixture()
	key := putNegativePartitionManifest(t, st, m, 20)
	other := ownershipPartitionManifestFixture()
	other.Parent.GroupID = 9
	for i := range other.Children {
		other.Children[i].GroupID = 9
	}
	raw, err := EncodeOwnershipPartitionManifest(other)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(t.Context(), key, raw, 21, 0))
	_, _, err = catalog.OwnershipPartitionNegativeAt(t.Context(), m.Parent.RouteID, m.Parent.ParentRouteID, m.Parent.Start, m.Parent.End, 21)
	require.ErrorIs(t, err, ErrOwnershipPartitionManifest)
}
