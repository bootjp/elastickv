package distribution

import (
	"bytes"
	"math"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func ownershipPartitionManifestFixture() OwnershipPartitionManifest {
	parent := RouteDescriptor{RouteID: 3, Start: []byte("m"), End: []byte("z"), GroupID: 2,
		State: RouteStateActive, MinWriteTSExclusive: 100, SplitAtHLC: 110}
	left, right := parent, parent
	left.RouteID, right.RouteID = 4, 5
	left.ParentRouteID, right.ParentRouteID = parent.RouteID, parent.RouteID
	left.End, right.Start = []byte("x"), []byte("x")
	left.SplitAtHLC, right.SplitAtHLC = 200, 200
	return OwnershipPartitionManifest{PreviousVersion: 12, Version: 13, Parent: parent,
		Children: []RouteDescriptor{left, right}}
}

func TestOwnershipPartitionManifestCanonicalCodec(t *testing.T) {
	manifest := ownershipPartitionManifestFixture()
	encoded, err := EncodeOwnershipPartitionManifest(manifest)
	require.NoError(t, err)
	decoded, err := DecodeOwnershipPartitionManifest(encoded)
	require.NoError(t, err)
	require.Equal(t, manifest, decoded)
	reversed := manifest
	reversed.Children = []RouteDescriptor{manifest.Children[1], manifest.Children[0]}
	other, err := EncodeOwnershipPartitionManifest(reversed)
	require.NoError(t, err)
	require.Equal(t, encoded, other, "child ordering must not change root operation identity")
	firstHash, err := OwnershipPartitionManifestHash(manifest)
	require.NoError(t, err)
	secondHash, err := OwnershipPartitionManifestHash(reversed)
	require.NoError(t, err)
	require.Equal(t, firstHash, secondHash)
	decoded.Children[0].Start[0] = 'n'
	require.Equal(t, []byte("m"), manifest.Children[0].Start, "decoding must not alias caller descriptors")
}

func TestOwnershipPartitionManifestRejectsAuthorityChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*OwnershipPartitionManifest)
	}{
		{"future version gap", func(m *OwnershipPartitionManifest) { m.Version++ }},
		{"version overflow", func(m *OwnershipPartitionManifest) { m.PreviousVersion = math.MaxUint64; m.Version = 0 }},
		{"owner change", func(m *OwnershipPartitionManifest) { m.Children[1].GroupID++ }},
		{"left floor change", func(m *OwnershipPartitionManifest) { m.Children[0].MinWriteTSExclusive++ }},
		{"floor decrease", func(m *OwnershipPartitionManifest) { m.Children[1].MinWriteTSExclusive-- }},
		{"hole", func(m *OwnershipPartitionManifest) { m.Children[1].Start = []byte("y") }},
		{"overlap", func(m *OwnershipPartitionManifest) { m.Children[1].Start = []byte("w") }},
		{"enlargement", func(m *OwnershipPartitionManifest) { m.Children[1].End = nil }},
		{"lost staged lineage", func(m *OwnershipPartitionManifest) {
			m.Parent.StagedVisibilityActive = true
			m.Parent.MigrationJobID = 7
		}},
		{"wrong parent", func(m *OwnershipPartitionManifest) { m.Children[0].ParentRouteID++ }},
		{"duplicate child", func(m *OwnershipPartitionManifest) { m.Children[1].RouteID = m.Children[0].RouteID }},
		{"reuse parent ID", func(m *OwnershipPartitionManifest) { m.Children[0].RouteID = m.Parent.RouteID }},
		{"fenced child", func(m *OwnershipPartitionManifest) { m.Children[1].State = RouteStateWriteFenced }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := ownershipPartitionManifestFixture()
			tc.mutate(&manifest)
			_, err := EncodeOwnershipPartitionManifest(manifest)
			require.ErrorIs(t, err, ErrOwnershipPartitionManifest)
		})
	}
}

func TestOwnershipPartitionManifestSurvivesJobGCAndUnrelatedCatalogVersion(t *testing.T) {
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	manifest := ownershipPartitionManifestFixture()
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(12), 10, 0))
	mutation, err := catalog.BuildOwnershipPartitionMutationAt(t.Context(), 10, manifest)
	require.NoError(t, err)
	require.NoError(t, st.ApplyMutations(t.Context(), []*store.KVPairMutation{mutation,
		{Op: store.OpTypePut, Key: CatalogVersionKey(), Value: EncodeCatalogVersion(13)}},
		[][]byte{CatalogVersionKey(), mutation.Key}, 10, 20))
	require.NoError(t, st.DeletePrefixAt(t.Context(), []byte(catalogSplitJobHistoryPrefix), nil, 30))
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(14), 40, 0))
	digest, err := OwnershipPartitionDescriptorHash(manifest.Parent)
	require.NoError(t, err)
	got, found, err := catalog.OwnershipPartitionAt(t.Context(), 3, digest, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, manifest, got)
	_, found, err = catalog.OwnershipPartitionAt(t.Context(), 3, digest, 10)
	require.NoError(t, err)
	require.False(t, found)
	wrongDigest := bytes.Repeat([]byte{1}, len(digest))
	_, found, err = catalog.OwnershipPartitionAt(t.Context(), 3, wrongDigest, math.MaxUint64)
	require.NoError(t, err)
	require.False(t, found, "route ID alone cannot authorize a changed descriptor")
	_, err = catalog.BuildOwnershipPartitionMutationAt(t.Context(), 40, manifest)
	require.ErrorIs(t, err, ErrCatalogVersionMismatch)
}

func TestOwnershipPartitionManifestCannotAppearAheadOfCommittedCatalog(t *testing.T) {
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	manifest := ownershipPartitionManifestFixture()
	digest, err := OwnershipPartitionDescriptorHash(manifest.Parent)
	require.NoError(t, err)
	key, err := CatalogOwnershipPartitionKey(3, digest)
	require.NoError(t, err)
	encoded, err := EncodeOwnershipPartitionManifest(manifest)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(t.Context(), key, encoded, 10, 0))
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(12), 10, 0))
	_, _, err = catalog.OwnershipPartitionAt(t.Context(), 3, digest, math.MaxUint64)
	require.ErrorIs(t, err, ErrOwnershipPartitionManifest)
}

func TestOwnershipPartitionManifestCatalogCASLeavesNoPartialLineage(t *testing.T) {
	st := store.NewMVCCStore()
	catalog := NewCatalogStore(st)
	manifest := ownershipPartitionManifestFixture()
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(12), 10, 0))
	mutation, err := catalog.BuildOwnershipPartitionMutationAt(t.Context(), 10, manifest)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(13), 15, 0))
	err = st.ApplyMutations(t.Context(), []*store.KVPairMutation{mutation,
		{Op: store.OpTypePut, Key: CatalogVersionKey(), Value: EncodeCatalogVersion(13)}},
		[][]byte{CatalogVersionKey(), mutation.Key}, 10, 20)
	require.ErrorIs(t, err, store.ErrWriteConflict)
	_, err = st.GetAt(t.Context(), mutation.Key, math.MaxUint64)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestOwnershipPartitionManifestNativeSnapshotRetainsLineage(t *testing.T) {
	st := store.NewMVCCStore()
	manifest := ownershipPartitionManifestFixture()
	digest, err := OwnershipPartitionDescriptorHash(manifest.Parent)
	require.NoError(t, err)
	key, err := CatalogOwnershipPartitionKey(3, digest)
	require.NoError(t, err)
	encoded, err := EncodeOwnershipPartitionManifest(manifest)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(t.Context(), key, encoded, 20, 0))
	require.NoError(t, st.PutAt(t.Context(), CatalogVersionKey(), EncodeCatalogVersion(13), 20, 0))
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	dir := t.TempDir()
	destination, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, destination.Restore(bytes.NewReader(raw.Bytes())))
	require.NoError(t, destination.Close())
	destination, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destination.Close()) })
	got, found, err := NewCatalogStore(destination).OwnershipPartitionAt(t.Context(), 3, digest, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, manifest, got)
}
