package store

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func sealedPartitionEvidenceFixture(t *testing.T, st MVCCStore) MigrationServingState {
	t.Helper()
	_, tx := partitionFixture(t, st)
	require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
	view, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	require.NotNil(t, view.Partitions.servingEvidence)
	return view
}

func TestPartitionEvidenceIndexRejectsCallerForgedAttestation(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		view := sealedPartitionEvidenceFixture(t, st)
		receipt := view.Partitions.Receipts[0]
		receipt.Transition.After[0].MinWriteTSExclusive--
		id, err := MigrationOwnershipPartitionReceiptIDFor(receipt.Transition)
		require.NoError(t, err)
		receipt.ReceiptID = id
		for i := range receipt.Transition.After {
			receipt.Transition.After[i].ReceiptID = bytes.Clone(id)
		}
		view.Partitions.Receipts = []MigrationOwnershipPartitionReceipt{receipt}
		view.Partitions.Partitions = receipt.Transition.After
		forged := view.Partitions.Partitions[0]
		require.True(t, migrationPartitionEvidenceUncached(view.Partitions, forged), "the caller recomputed a self-consistent, but never durable, receipt")
		require.False(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, forged), "public receipt copies cannot replace sealed durable authority")
		fresh, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.True(t, MigrationOwnershipPartitionEvidenceValid(fresh.Partitions, fresh.Partitions.Partitions[0]))
		require.EqualValues(t, 100, fresh.Partitions.Partitions[0].MinWriteTSExclusive)
	})
}

func TestPartitionEvidenceIndexRequiresCurrentActiveMember(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		view := sealedPartitionEvidenceFixture(t, st)
		left, right := view.Partitions.Partitions[0], view.Partitions.Partitions[1]
		view.Partitions.Partitions = view.Partitions.Partitions[1:]
		require.False(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, left))
		require.True(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, right))
		view.Partitions.Partitions[0].Role = MigrationOwner
		view.Partitions.Partitions[0].OwnerGroupID = right.LocalGroupID
		require.False(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, right), "an ID match cannot hide a changed active member")
		require.False(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, view.Partitions.Partitions[0]))
		view.Partitions.Generation = "different-generation"
		require.False(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, right))
	})
}

func TestPartitionEvidenceIndexNeverSerializesTrust(t *testing.T) {
	view := sealedPartitionEvidenceFixture(t, NewMVCCStore())
	encoded, err := json.Marshal(view.Partitions)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "servingEvidence")
	var restored MigrationOwnershipPartitionState
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Nil(t, restored.servingEvidence)
	for _, p := range restored.Partitions {
		require.True(t, MigrationOwnershipPartitionEvidenceValid(restored, p), "unsealed state still proves full receipt evidence")
	}
	restored.Receipts[0].Transition.After[0].Origins[0].JobID++
	index := buildPartitionEvidenceIndex(restored)
	restored.servingEvidence = index
	for _, p := range restored.Partitions {
		require.False(t, MigrationOwnershipPartitionEvidenceValid(restored, p), "cache construction must not seal corrupt support")
	}
}

func TestPartitionEvidenceIndexClonesIsolateConcurrentMutation(t *testing.T) {
	st := NewMVCCStore()
	mutated := sealedPartitionEvidenceFixture(t, st)
	stable, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	require.Same(t, mutated.Partitions.servingEvidence, stable.Partitions.servingEvidence)
	var done sync.WaitGroup
	done.Go(func() {
		for range 100 {
			mutated.Partitions.Partitions[0].DescriptorHash[0]++
			mutated.Partitions.Receipts[0].Transition.After[0].Origins[0].DescriptorHash[0]++
		}
	})
	for range 100 {
		require.True(t, MigrationOwnershipPartitionEvidenceValid(stable.Partitions, stable.Partitions.Partitions[0]))
	}
	done.Wait()
}
