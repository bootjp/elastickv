package store

import (
	"bytes"
	"encoding/hex"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func codecPropertyPartitionFixture(t *testing.T) ([]MigrationOwnershipState, MigrationOwnershipPartitionTransition) {
	t.Helper()
	st := NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	old, transition := partitionFixture(t, st)
	newer := old
	newer.JobID, newer.CatalogVersion, newer.OwnerGroupID, newer.Role = 8, 11, 1, MigrationOwner
	newer.RouteStart = []byte("m")
	newer.DescriptorHash = bytes.Repeat([]byte{5}, 32)
	require.NoError(t, ownershipStore(t, st).ClearMigrationStateWithOwnership(t.Context(), newer, 10))
	view, err := ownershipStore(t, st).MigrationServingState(t.Context())
	require.NoError(t, err)
	ref, err := MigrationOwnershipLegacyReferenceFor(view.Ownership[1])
	require.NoError(t, err)
	transition.Before = append(transition.Before, ref)
	transition.After[1] = partitionFromLegacy(view.Ownership[1])
	finalizePartitionTransition(t, &transition)
	return view.Ownership, transition
}

func codecPropertyPartitionState(t *rapid.T, ledger []MigrationOwnershipState, seed MigrationOwnershipPartitionTransition) MigrationOwnershipPartitionState {
	t.Helper()
	transition := canonicalPartitionTransition(seed)
	transition.OperationID = "codec/split/" + hex.EncodeToString(rapid.SliceOfN(rapid.Byte(), 0, 24).Draw(t, "operation"))
	transition.RootManifestHash = rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, "root hash")
	transition.CatalogVersion += rapid.Uint64Range(0, 10000).Draw(t, "catalog increment")
	transition.ConfigurationIndex += rapid.Uint64Range(0, 10000).Draw(t, "configuration increment")
	index := rapid.Uint64Range(100, 1000000).Draw(t, "apply index")
	for i := range transition.After {
		child := &transition.After[i]
		child.CatalogVersion, child.ConfigurationIndex = transition.CatalogVersion, transition.ConfigurationIndex
		child.DescriptorHash = rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, "child descriptor hash")
		id, err := MigrationOwnershipPartitionIDFor(transition, i)
		require.NoError(t, err)
		child.PartitionID = id
	}
	state, err := applyPartitionTransition(MigrationOwnershipPartitionState{}, ledger, transition, index, transition.ConfigurationIndex)
	require.NoError(t, err)
	child := state.Partitions[rapid.IntRange(0, len(state.Partitions)-1).Draw(t, "refreshed child")]
	ref, err := MigrationOwnershipPartitionReferenceFor(child)
	require.NoError(t, err)
	refresh := MigrationOwnershipPartitionTransition{
		OperationID: "codec/refresh", Operation: MigrationOwnershipPartitionRefresh,
		Generation: state.Generation, LocalGroupID: child.LocalGroupID,
		CatalogVersion: transition.CatalogVersion + 1, ConfigurationIndex: transition.ConfigurationIndex,
		RootManifestHash: transition.RootManifestHash, Before: []MigrationOwnershipPartitionReference{ref},
		After: []MigrationOwnershipPartition{clonePartition(child)},
	}
	refresh.After[0].CatalogVersion = refresh.CatalogVersion
	state, err = applyPartitionTransition(state, ledger, refresh, index+1, refresh.ConfigurationIndex)
	require.NoError(t, err)
	return state
}

func codecPropertyPermute[T any](t *rapid.T, values []T) {
	t.Helper()
	for i := len(values) - 1; i > 0; i-- {
		j := rapid.IntRange(0, i).Draw(t, "permutation")
		values[i], values[j] = values[j], values[i]
	}
}

func TestMigrationCodecPropertyPartitionRoundTripAndHashes(t *testing.T) {
	ledger, seed := codecPropertyPartitionFixture(t)
	rapid.Check(t, func(t *rapid.T) {
		state := codecPropertyPartitionState(t, ledger, seed)
		encoded, err := encodeMigrationOwnershipPartitions(state)
		require.NoError(t, err)
		decoded, err := decodeMigrationOwnershipPartitions(encoded)
		require.NoError(t, err)
		require.Equal(t, state, decoded)
		for _, child := range decoded.Partitions {
			require.True(t, MigrationOwnershipPartitionEvidenceValid(decoded, child))
		}
		permuted := clonePartitionState(state)
		codecPropertyPermute(t, permuted.Partitions)
		codecPropertyPermute(t, permuted.Receipts)
		for i := range permuted.Receipts {
			transition := &permuted.Receipts[i].Transition
			codecPropertyPermute(t, transition.Before)
			codecPropertyPermute(t, transition.After)
			id, hashErr := MigrationOwnershipPartitionReceiptIDFor(*transition)
			require.NoError(t, hashErr)
			require.Equal(t, permuted.Receipts[i].ReceiptID, id)
			for j, child := range transition.After {
				id, hashErr = MigrationOwnershipPartitionIDFor(*transition, j)
				require.NoError(t, hashErr)
				require.Equal(t, child.PartitionID, id)
			}
		}
		reencoded, err := encodeMigrationOwnershipPartitions(permuted)
		require.NoError(t, err)
		require.Equal(t, encoded, reencoded)
		codecPropertyPartitionHashBinding(t, state.Receipts[0].Transition)
	})
}

func codecPropertyPartitionHashBinding(t *rapid.T, transition MigrationOwnershipPartitionTransition) {
	t.Helper()
	id, err := MigrationOwnershipPartitionReceiptIDFor(transition)
	require.NoError(t, err)
	changed := canonicalPartitionTransition(transition)
	changed.RootManifestHash[0] ^= 1
	different, err := MigrationOwnershipPartitionReceiptIDFor(changed)
	require.NoError(t, err)
	require.NotEqual(t, id, different)
	stamped := canonicalPartitionTransition(transition)
	for i := range stamped.After {
		stamped.After[i].AppliedIndex = rapid.Uint64().Draw(t, "ignored apply stamp")
		stamped.After[i].ReceiptID = []byte("ignored receipt stamp")
	}
	same, err := MigrationOwnershipPartitionReceiptIDFor(stamped)
	require.NoError(t, err)
	require.Equal(t, id, same, "receipt identity excludes apply-time stamps")
}

func TestMigrationCodecPropertyPartitionRejectsMalformed(t *testing.T) {
	ledger, seed := codecPropertyPartitionFixture(t)
	rapid.Check(t, func(t *rapid.T) {
		state := codecPropertyPartitionState(t, ledger, seed)
		mutations := []struct {
			name   string
			mutate func(*MigrationOwnershipPartitionState)
		}{
			{"generation", func(s *MigrationOwnershipPartitionState) { s.Generation += "different" }},
			{"receipt digest", func(s *MigrationOwnershipPartitionState) { s.Receipts[0].ReceiptID[0] ^= 1 }},
			{"duplicate receipt", func(s *MigrationOwnershipPartitionState) { s.Receipts = append(s.Receipts, s.Receipts[0]) }},
			{"missing receipts", func(s *MigrationOwnershipPartitionState) { s.Receipts = nil }},
			{"frontier stamp", func(s *MigrationOwnershipPartitionState) { s.Partitions[0].AppliedIndex++ }},
			{"unknown floor", func(s *MigrationOwnershipPartitionState) { s.Partitions[0].WriteFloorKnown = false }},
			{"duplicate frontier", func(s *MigrationOwnershipPartitionState) { s.Partitions = append(s.Partitions, s.Partitions[0]) }},
		}
		for _, mutation := range mutations {
			bad := clonePartitionState(state)
			mutation.mutate(&bad)
			encoded, err := encodeMigrationOwnershipPartitions(bad)
			require.NoError(t, err)
			_, err = decodeMigrationOwnershipPartitions(encoded)
			require.Error(t, err, mutation.name)
		}
		encoded, err := encodeMigrationOwnershipPartitions(state)
		require.NoError(t, err)
		for _, bad := range [][]byte{slices.Clone(encoded[:len(encoded)-1]), append([]byte{0}, encoded[1:]...), append(slices.Clone(encoded), '{')} {
			_, err = decodeMigrationOwnershipPartitions(bad)
			require.Error(t, err)
		}
	})
}
