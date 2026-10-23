package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func changedNegativeManifest(t *testing.T, c MigrationPartitionControl) distribution.OwnershipPartitionManifest {
	t.Helper()
	m, err := distribution.DecodeOwnershipPartitionManifest(c.Manifest)
	require.NoError(t, err)
	m.Parent.GroupID, m.Parent.MinWriteTSExclusive = 3, 200
	for i := range m.Children {
		m.Children[i].GroupID, m.Children[i].MinWriteTSExclusive = 3, 200
	}
	return m
}

func TestPartitionNegativeGeometryPreservesHistoricalDenialAndRefresh(t *testing.T) {
	st := store.NewMVCCStore()
	f, old := partitionControlFixture(t, st, 2)
	m := changedNegativeManifest(t, old)
	raw, err := distribution.EncodeOwnershipPartitionManifest(m)
	require.NoError(t, err)
	c, err := BuildMigrationPartitionControl(nil, old.PriorOwnership, raw, nil, 40, 9)
	require.NoError(t, err)
	f.SetApplyIndex(20)
	require.IsType(t, store.CleanupVersionsResult{}, f.Apply(partitionControlCommand(t, c)))
	reader, ok := st.(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	before, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	for _, p := range before.Partitions {
		require.Equal(t, store.MigrationNonOwner, p.Role)
		require.EqualValues(t, 1, p.OwnerGroupID, "root owner 3 is not an ownership acquisition")
		require.EqualValues(t, 100, p.MinWriteTSExclusive, "never replace the native known floor with a guessed one")
		require.EqualValues(t, 11, p.MigrationJobID)
		require.True(t, store.MigrationOwnershipPartitionEvidenceValid(before, p))
	}
	refresh, err := BuildMigrationPartitionControl(before.Partitions[:1], nil, nil, &m.Children[0], 41, 9)
	require.NoError(t, err, "already projected negative hash can refresh without gaining the root's owner/floor")
	f.SetApplyIndex(21)
	require.IsType(t, store.CleanupVersionsResult{}, f.Apply(partitionControlCommand(t, refresh)))
	after, err := reader.MigrationOwnershipPartitions(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Partitions[0].PartitionID, after.Partitions[0].PartitionID)
	require.Equal(t, before.Partitions[0].Origins, after.Partitions[0].Origins)
	require.Equal(t, before.Partitions[0].OwnerGroupID, after.Partitions[0].OwnerGroupID)
	require.Equal(t, before.Partitions[0].MinWriteTSExclusive, after.Partitions[0].MinWriteTSExclusive)
}

func TestPartitionNegativeGeometryRejectsAuthorityAndGeometryChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*distribution.OwnershipPartitionManifest, []store.MigrationOwnershipState)
	}{
		{"root owns local", func(m *distribution.OwnershipPartitionManifest, _ []store.MigrationOwnershipState) {
			m.Parent.GroupID = 2
			for i := range m.Children {
				m.Children[i].GroupID = 2
			}
		}},
		{"floor lowered", func(m *distribution.OwnershipPartitionManifest, _ []store.MigrationOwnershipState) {
			m.Parent.MinWriteTSExclusive = 99
			for i := range m.Children {
				m.Children[i].MinWriteTSExclusive = 99
			}
		}},
		{"parent ID changed", func(m *distribution.OwnershipPartitionManifest, _ []store.MigrationOwnershipState) {
			m.Parent.ParentRouteID++
		}},
		{"bounds changed", func(m *distribution.OwnershipPartitionManifest, _ []store.MigrationOwnershipState) {
			m.Parent.Start = []byte("b")
			m.Children[0].Start = []byte("b")
		}},
		{"unknown floor", func(_ *distribution.OwnershipPartitionManifest, prior []store.MigrationOwnershipState) {
			prior[0].MinWriteTSExclusive = 0
			prior[0].WriteFloorKnown = false
		}},
		{"SOURCE record", func(_ *distribution.OwnershipPartitionManifest, prior []store.MigrationOwnershipState) {
			prior[0].SourceTrackerVersion = 1
			prior[0].SourceAppliedIndex = 10
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, old := partitionControlFixture(t, store.NewMVCCStore(), 2)
			m := changedNegativeManifest(t, old)
			tc.mutate(&m, old.PriorOwnership)
			raw, err := distribution.EncodeOwnershipPartitionManifest(m)
			require.NoError(t, err)
			_, err = BuildMigrationPartitionControl(nil, old.PriorOwnership, raw, nil, 40, 9)
			require.Error(t, err)
		})
	}
}

func TestPartitionNegativeGeometryRejectsAnyOwnerInCompletePrior(t *testing.T) {
	_, old := partitionControlFixture(t, store.NewMVCCStore(), 2)
	negative := old.PriorOwnership[0]
	negative.CatalogVersion = 2
	positive := old.PriorOwnership[0]
	positive.JobID, positive.OwnerGroupID, positive.Role = 12, 2, store.MigrationOwner
	require.True(t, store.MigrationOwnershipEvidenceValid(positive))
	prior := []store.MigrationOwnershipState{positive, negative}
	m := changedNegativeManifest(t, old)
	raw, err := distribution.EncodeOwnershipPartitionManifest(m)
	require.NoError(t, err)
	require.False(t, MigrationPartitionNegativePriors(nil, prior))
	_, err = BuildMigrationPartitionControl(nil, prior, raw, nil, 40, 9)
	require.Error(t, err, "an older OWNER cannot be omitted merely because the newest covering record is NONOWNER")
	_, owner := partitionControlFixture(t, store.NewMVCCStore(), 1)
	_, err = BuildMigrationPartitionControl(nil, owner.PriorOwnership, raw, nil, 40, 9)
	require.Error(t, err, "positive proofs retain exact descriptor/owner/floor matching")
}
