package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func partitionFixture(t *testing.T, st MVCCStore) (MigrationOwnershipState, MigrationOwnershipPartitionTransition) {
	t.Helper()
	old := ownershipFixture(t, st)
	old.Generation = "0123456789abcdef0123456789abcdef"
	old.RouteID = 10
	old.DescriptorHash = bytes.Repeat([]byte{1}, 32)
	old.WriteFloorKnown = true
	require.NoError(t, ownershipStore(t, st).ClearMigrationStateWithOwnership(t.Context(), old, 9))
	require.NoError(t, st.PutAt(t.Context(), MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 3), 1, 0))
	view, err := ownershipStore(t, st).MigrationServingState(t.Context())
	require.NoError(t, err)
	old = view.Ownership[0]
	ref, err := MigrationOwnershipLegacyReferenceFor(old)
	require.NoError(t, err)
	left, right := partitionFromLegacy(old), partitionFromLegacy(old)
	left.RouteID, left.ParentRouteID, left.RouteEnd, left.DescriptorHash = 11, 10, []byte("m"), bytes.Repeat([]byte{2}, 32)
	right.RouteID, right.ParentRouteID, right.RouteStart, right.DescriptorHash = 12, 10, []byte("m"), bytes.Repeat([]byte{3}, 32)
	tx := MigrationOwnershipPartitionTransition{OperationID: "root/split/10", Operation: MigrationOwnershipPartitionSplit,
		RootManifestHash: bytes.Repeat([]byte{4}, 32), Generation: old.Generation, LocalGroupID: 1, CatalogVersion: 12,
		ConfigurationIndex: 3, Before: []MigrationOwnershipPartitionReference{ref}, After: []MigrationOwnershipPartition{left, right}}
	finalizePartitionTransition(t, &tx)
	return old, tx
}

func finalizePartitionTransition(t *testing.T, tx *MigrationOwnershipPartitionTransition) {
	t.Helper()
	for i := range tx.After {
		tx.After[i].CatalogVersion, tx.After[i].ConfigurationIndex = tx.CatalogVersion, tx.ConfigurationIndex
		id, err := MigrationOwnershipPartitionIDFor(*tx, i)
		require.NoError(t, err)
		tx.After[i].PartitionID = id
		tx.After[i].ReceiptID, tx.After[i].AppliedIndex = nil, 0
	}
}

func partitionStore(t *testing.T, st MVCCStore) MigrationOwnershipPartitionStore {
	t.Helper()
	p, ok := st.(MigrationOwnershipPartitionStore)
	require.True(t, ok)
	return p
}

func TestOwnershipPartitionAtomicSplitRefreshAndLateReplay(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		old, tx := partitionFixture(t, st)
		proofs := partitionStore(t, st)
		require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		view, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Equal(t, []MigrationOwnershipState{old}, view.Ownership, "job ledger is not rewritten as children")
		require.Len(t, view.Partitions.Partitions, 2)
		require.Len(t, view.Partitions.Receipts, 1)
		for _, p := range view.Partitions.Partitions {
			require.True(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, p))
			require.Equal(t, MigrationNonOwner, p.Role)
			require.Equal(t, old.MinWriteTSExclusive, p.MinWriteTSExclusive)
			require.Equal(t, partitionFromLegacy(old).Origins, p.Origins)
		}
		ref, err := MigrationOwnershipPartitionReferenceFor(view.Partitions.Partitions[0])
		require.NoError(t, err)
		refresh := tx
		refresh.OperationID, refresh.Operation, refresh.CatalogVersion = "root/refresh/13", MigrationOwnershipPartitionRefresh, 13
		refresh.Before = []MigrationOwnershipPartitionReference{ref}
		refresh.After = []MigrationOwnershipPartition{view.Partitions.Partitions[0]}
		finalizePartitionTransition(t, &refresh)
		require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), refresh, 11))
		newer, err := proofs.MigrationOwnershipPartitions(t.Context())
		require.NoError(t, err)
		require.False(t, MigrationOwnershipPartitionEvidenceValid(newer, view.Partitions.Partitions[0]), "immutable history does not make a replaced attestation active")
		require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), tx, 12), "immutable receipt permits late duplicate without rollback")
		after, err := proofs.MigrationOwnershipPartitions(t.Context())
		require.NoError(t, err)
		require.Equal(t, newer, after)
		conflict := canonicalPartitionTransition(tx)
		conflict.RootManifestHash[0]++
		require.ErrorIs(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), conflict, 13), ErrMigrationOwnership)
		if ps, ok := st.(*pebbleStore); ok {
			index, found, readErr := ps.LastAppliedIndex()
			require.NoError(t, readErr)
			require.True(t, found)
			require.Equal(t, uint64(12), index)
			require.NoError(t, ps.SetDurableAppliedIndex(99))
			require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
			index, found, readErr = ps.LastAppliedIndex()
			require.NoError(t, readErr)
			require.True(t, found)
			require.Equal(t, uint64(99), index, "late duplicate cannot rewind an unrelated later Raft watermark")
		}
		view.Partitions.Partitions[0].Origins[0].DescriptorHash[0]++
		view.Partitions.Receipts[0].Transition.After[0].DescriptorHash[0]++
		unchanged, err := proofs.MigrationOwnershipPartitions(t.Context())
		require.NoError(t, err)
		require.Equal(t, newer, unchanged, "readers own all returned slices")
	})
}

func TestOwnershipPartitionRejectsBeforeEffects(t *testing.T) {
	cases := map[string]func(*MigrationOwnershipPartitionTransition){
		"owner grant": func(tx *MigrationOwnershipPartitionTransition) {
			tx.After[0].Role, tx.After[0].OwnerGroupID = MigrationOwner, 1
		},
		"floor decrease":   func(tx *MigrationOwnershipPartitionTransition) { tx.After[0].MinWriteTSExclusive-- },
		"floor unknown":    func(tx *MigrationOwnershipPartitionTransition) { tx.After[0].WriteFloorKnown = false },
		"floor increase":   func(tx *MigrationOwnershipPartitionTransition) { tx.After[0].MinWriteTSExclusive++ },
		"hole":             func(tx *MigrationOwnershipPartitionTransition) { tx.After[1].RouteStart = []byte("n") },
		"overlap":          func(tx *MigrationOwnershipPartitionTransition) { tx.After[1].RouteStart = []byte("l") },
		"enlargement":      func(tx *MigrationOwnershipPartitionTransition) { tx.After[1].RouteEnd = nil },
		"missing before":   func(tx *MigrationOwnershipPartitionTransition) { tx.Before = nil },
		"duplicate before": func(tx *MigrationOwnershipPartitionTransition) { tx.Before = append(tx.Before, tx.Before[0]) },
		"forged digest":    func(tx *MigrationOwnershipPartitionTransition) { tx.Before[0].AttestationHash[0]++ },
		"forged origin":    func(tx *MigrationOwnershipPartitionTransition) { tx.After[0].Origins[0].JobID++ },
		"lost lineage":     func(tx *MigrationOwnershipPartitionTransition) { tx.After[0].MigrationJobID++ },
		"generation":       func(tx *MigrationOwnershipPartitionTransition) { tx.Generation = "different-generation" },
		"configuration":    func(tx *MigrationOwnershipPartitionTransition) { tx.ConfigurationIndex++ },
		"unknown epoch":    func(tx *MigrationOwnershipPartitionTransition) { tx.ConfigurationIndex = 0 },
		"manifest":         func(tx *MigrationOwnershipPartitionTransition) { tx.RootManifestHash = nil },
		"external job op": func(tx *MigrationOwnershipPartitionTransition) {
			tx.Operation = MigrationOwnershipPartitionJobOperation
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
				_, tx := partitionFixture(t, st)
				before, err := ownershipStore(t, st).MigrationServingState(t.Context())
				require.NoError(t, err)
				mutate(&tx)
				finalizePartitionTransition(t, &tx)
				require.ErrorIs(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10), ErrMigrationOwnership)
				after, err := ownershipStore(t, st).MigrationServingState(t.Context())
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		})
	}
}

func TestOwnershipPartitionRequiresAllLegacyOverlays(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		old, tx := partitionFixture(t, st)
		newer := old
		newer.JobID, newer.CatalogVersion, newer.OwnerGroupID, newer.Role = 8, 11, 1, MigrationOwner
		newer.RouteStart, newer.RouteEnd = []byte("m"), []byte("z")
		newer.DescriptorHash = bytes.Repeat([]byte{5}, 32)
		require.NoError(t, ownershipStore(t, st).ClearMigrationStateWithOwnership(t.Context(), newer, 10))
		require.ErrorIs(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 11), ErrMigrationOwnership,
			"omitted newer positive/negative overlays may not disappear")
		view, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		ref, err := MigrationOwnershipLegacyReferenceFor(view.Ownership[1])
		require.NoError(t, err)
		tx.Before = append(tx.Before, ref)
		tx.After[1] = partitionFromLegacy(view.Ownership[1])
		finalizePartitionTransition(t, &tx)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 11))
	})
}

func TestOwnershipPartitionUnboundedUnionAndUnknownLegacy(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		old, tx := partitionFixture(t, st)
		old.RouteStart, old.RouteEnd, old.JobID, old.CatalogVersion = nil, nil, 8, 11
		require.NoError(t, ownershipStore(t, st).ClearMigrationStateWithOwnership(t.Context(), old, 10))
		view, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		ref, err := MigrationOwnershipLegacyReferenceFor(view.Ownership[1])
		require.NoError(t, err)
		tx.Before = append(tx.Before, ref)
		tx.After = []MigrationOwnershipPartition{partitionFromLegacy(view.Ownership[1])}
		finalizePartitionTransition(t, &tx)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 11))
	})
	st := NewMVCCStore()
	_, tx := partitionFixture(t, st)
	ms, ok := st.(*mvccStore)
	require.True(t, ok)
	ms.migrationOwnership[0].Receipts = nil
	require.ErrorIs(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10), ErrMigrationOwnership)
}
