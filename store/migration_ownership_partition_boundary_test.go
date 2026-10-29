package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

func TestOwnershipPartitionSourceZeroFloorAndConfigurationCAS(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		guard := TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true,
			RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1, MinWriteTSExclusive: 1, RetentionPinTS: 1}
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 10))
		state := MigrationOwnershipState{Generation: "0123456789abcdef0123456789abcdef", JobID: 7, LocalGroupID: 1, OwnerGroupID: 1,
			Role: MigrationOwner, CatalogVersion: 1, ConfigurationIndex: 9, RouteID: 1, DescriptorHash: bytes.Repeat([]byte{1}, 32),
			RouteStart: []byte("m"), RouteEnd: []byte("z"), WriteFloorKnown: true, SourceTrackerVersion: 1}
		require.NoError(t, ownershipStore(t, st).AcquireMigrationOwnership(t.Context(), state, 11))
		require.NoError(t, st.PutAt(t.Context(), MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 9), 1, 0))
		view, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		ref, err := MigrationOwnershipLegacyReferenceFor(view.Ownership[0])
		require.NoError(t, err)
		left, right := partitionFromLegacy(view.Ownership[0]), partitionFromLegacy(view.Ownership[0])
		left.RouteID, left.RouteEnd, left.DescriptorHash = 2, []byte("x"), bytes.Repeat([]byte{2}, 32)
		right.RouteID, right.RouteStart, right.DescriptorHash = 3, []byte("x"), bytes.Repeat([]byte{3}, 32)
		tx := MigrationOwnershipPartitionTransition{OperationID: "source/split", Operation: MigrationOwnershipPartitionSplit,
			RootManifestHash: bytes.Repeat([]byte{4}, 32), Generation: state.Generation, LocalGroupID: 1, CatalogVersion: 2,
			ConfigurationIndex: 9, Before: []MigrationOwnershipPartitionReference{ref}, After: []MigrationOwnershipPartition{left, right}}
		finalizePartitionTransition(t, &tx)
		proofs := partitionStore(t, st)
		require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), tx, 12))
		frontier, err := proofs.MigrationOwnershipPartitions(t.Context())
		require.NoError(t, err)
		for _, p := range frontier.Partitions {
			require.Zero(t, p.MinWriteTSExclusive)
			require.True(t, p.WriteFloorKnown)
			require.Equal(t, uint64(11), p.SourceAppliedIndex)
			require.True(t, MigrationOwnershipPartitionEvidenceValid(frontier, p))
		}
		ref, err = MigrationOwnershipPartitionReferenceFor(frontier.Partitions[0])
		require.NoError(t, err)
		refresh := tx
		refresh.OperationID, refresh.Operation, refresh.CatalogVersion = "source/refresh", MigrationOwnershipPartitionRefresh, 3
		refresh.Before, refresh.After = []MigrationOwnershipPartitionReference{ref}, []MigrationOwnershipPartition{frontier.Partitions[0]}
		finalizePartitionTransition(t, &refresh)
		require.NoError(t, st.PutAt(t.Context(), MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 10), 2, 0))
		require.ErrorIs(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), refresh, 13), ErrMigrationOwnership, "durable epoch changed after proposal")
		refresh.ConfigurationIndex = 10
		finalizePartitionTransition(t, &refresh)
		require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), refresh, 13))
		current, err := proofs.MigrationOwnershipPartitions(t.Context())
		require.NoError(t, err)
		require.Equal(t, frontier.Partitions[1], current.Partitions[1], "unrelated right remains unchanged")
		require.NoError(t, proofs.ApplyMigrationOwnershipPartition(t.Context(), tx, 14), "old receipt replay cannot roll the new configuration back")
		current, err = proofs.MigrationOwnershipPartitions(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(10), current.Partitions[0].ConfigurationIndex)
	})
}

func TestOwnershipPartitionRootManifestExternalRefusal(t *testing.T) {
	key := append([]byte("!dist|meta|ownership_partition/"), make([]byte, 40)...)
	for _, format := range []string{"mvcc", "native", "sst", "encrypted-native", "encrypted-sst"} {
		t.Run(format, func(t *testing.T) {
			var raw []byte
			var options []PebbleStoreOption
			if format == "encrypted-native" || format == "encrypted-sst" {
				fixture := newEncryptedStoreFixture(t, 7)
				ps, ok := fixture.mvcc.(*pebbleStore)
				require.True(t, ok)
				ps.sstIngestSnapshots = format == "encrypted-sst"
				require.NoError(t, fixture.mvcc.PutAt(t.Context(), key, []byte("root manifest"), 1, 0))
				raw = externalTestSnapshot(t, fixture.mvcc)
				options = []PebbleStoreOption{WithEncryption(fixture.cipher, encryption.NewDeterministicNonceFactory(0xABCD, 2), func() (uint32, bool) { return fixture.keyID, true })}
			} else {
				source := externalTestMemory(t)
				require.NoError(t, source.PutAt(t.Context(), key, []byte("root manifest"), 1, 0))
				raw = externalTestFormat(t, source, format)
			}
			destination, err := NewPebbleStore(t.TempDir(), options...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, destination.Close()) })
			require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 1, 0))
			before := externalTestSnapshot(t, destination)
			require.ErrorIs(t, externalTestRestore(t, destination, raw), ErrExternalSnapshotMigrationState)
			require.Equal(t, before, externalTestSnapshot(t, destination))
			if format == "mvcc" {
				require.ErrorIs(t, externalTestRestore(t, NewMVCCStore(), raw), ErrExternalSnapshotMigrationState)
			}
		})
	}
	require.False(t, isMigrationMetadataKey(key), "root manifests are logical rows, never raw store containers")
	require.NoError(t, validateExternalTypedMigrationKey([]byte("!dist|meta|ownership_partition-user")))
}

func TestOwnershipPartitionHashesCanonicalAndBound(t *testing.T) {
	st := NewMVCCStore()
	_, tx := partitionFixture(t, st)
	id, err := MigrationOwnershipPartitionReceiptIDFor(tx)
	require.NoError(t, err)
	permuted := canonicalPartitionTransition(tx)
	permuted.After[0], permuted.After[1] = permuted.After[1], permuted.After[0]
	permuted.After[0].AppliedIndex, permuted.After[0].ReceiptID = 99, bytes.Repeat([]byte{9}, 32)
	other, err := MigrationOwnershipPartitionReceiptIDFor(permuted)
	require.NoError(t, err)
	require.Equal(t, id, other, "sorting and store-stamped boundaries do not alter operation identity")
	permuted.After[0].Origins[0].AppliedIndex++
	other, err = MigrationOwnershipPartitionReceiptIDFor(permuted)
	require.NoError(t, err)
	require.NotEqual(t, id, other, "immutable origin boundary is bound")
	key := MigrationOwnershipPartitionMetadataKey()
	key[0]++
	require.True(t, IsMigrationOwnershipPartitionMetadataKey([]byte("!migstage|ownership_partition")))
}
