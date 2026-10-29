package store

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

func applyPartitionNamespaceMarker(t *testing.T, st MVCCStore, configuration, index uint64) []byte {
	t.Helper()
	marker, err := EncodeMigrationNamespaceConfiguration(MigrationNamespaceConfiguration{
		Index: configuration, Version: MigrationNamespaceVersion, ActivatedAt: 3, Members: []uint64{1, 2, 3}, Joining: []uint64{4},
	})
	require.NoError(t, err)
	require.Greater(t, len(marker), 8, "activated state must exercise the full marker, not a legacy uint64")
	ts := st.LastCommitTS() + 1
	require.NoError(t, st.ApplyMutationsRaftAt(t.Context(), []*KVPairMutation{{
		Op: OpTypePut, Key: MigrationConfigurationIndexKey(), Value: marker,
	}}, nil, ts, ts, index))
	return marker
}

func TestOwnershipPartitionActivatedNamespaceGuardedAcquireClear(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		applyPartitionNamespaceMarker(t, st, 3, 10)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 11))
		before, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		incoming := MigrationOwnershipState{Generation: tx.Generation, JobID: 8, LocalGroupID: 1, OwnerGroupID: 1,
			CatalogVersion: 14, ConfigurationIndex: 3, RouteID: 12, ParentRouteID: 10, RouteStart: []byte("m"), RouteEnd: []byte("z"),
			DescriptorHash: bytes.Repeat([]byte{3}, 32), Role: MigrationOwner, MinWriteTSExclusive: 200,
			WriteFloorKnown: true, SourceTrackerVersion: 14}
		guard := TargetStagedReadinessState{JobID: 8, MigrationJobID: 8, Armed: true, TrackWrites: true,
			RouteStart: incoming.RouteStart, RouteEnd: incoming.RouteEnd, ExpectedCutoverVersion: 14,
			MinWriteTSExclusive: 200, RetentionPinTS: 1}
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 12))
		require.NoError(t, ownershipStore(t, st).AcquireMigrationOwnership(t.Context(), incoming, 13))
		acquired, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, before.Partitions.Partitions[0], acquired.Partitions.Partitions[0])
		require.Equal(t, uint64(13), acquired.Partitions.Partitions[1].SourceAppliedIndex)
		require.True(t, MigrationOwnershipPartitionEvidenceValid(acquired.Partitions, acquired.Partitions.Partitions[1]))
		guard.SourceWriteFence, guard.SourceReadFence, guard.ExpectedCutoverVersion = true, true, 15
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 14))
		outgoing := incoming
		outgoing.Role, outgoing.OwnerGroupID, outgoing.CatalogVersion, outgoing.SourceTrackerVersion = MigrationNonOwner, 2, 15, 0
		outgoing.DescriptorHash = bytes.Repeat([]byte{9}, 32)
		require.NoError(t, ownershipStore(t, st).ClearMigrationStateWithOwnership(t.Context(), outgoing, 15))
		cleared, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, MigrationNonOwner, cleared.Partitions.Partitions[1].Role)
		require.Empty(t, cleared.Readiness)
		require.Equal(t, before.Partitions.Partitions[0], cleared.Partitions.Partitions[0])
	})
}

func TestOwnershipPartitionActivatedNamespaceConfigurationCAS(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		applyPartitionNamespaceMarker(t, st, 3, 10)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 11))
		before, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		p := before.Partitions.Partitions[0]
		ref, err := MigrationOwnershipPartitionReferenceFor(p)
		require.NoError(t, err)
		refresh := tx
		refresh.Operation, refresh.OperationID, refresh.CatalogVersion = MigrationOwnershipPartitionRefresh, "activated/refresh", 13
		refresh.Before, refresh.After = []MigrationOwnershipPartitionReference{ref}, []MigrationOwnershipPartition{p}
		finalizePartitionTransition(t, &refresh)
		applyPartitionNamespaceMarker(t, st, 4, 12)
		require.ErrorIs(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), refresh, 13), ErrMigrationOwnership)
		after, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, before, after, "a stale configuration does not publish frontier or receipts")
		refresh.ConfigurationIndex = 4
		finalizePartitionTransition(t, &refresh)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), refresh, 13))
		after, err = admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(4), after.Partitions.Partitions[0].ConfigurationIndex)
		require.Equal(t, before.Partitions.Partitions[1], after.Partitions.Partitions[1])
	})
}

func TestOwnershipPartitionActivatedNamespaceCorruptionRejectsBeforeEffects(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		marker := applyPartitionNamespaceMarker(t, st, 3, 10)
		ts := st.LastCommitTS() + 1
		require.NoError(t, st.ApplyMutationsRaftAt(t.Context(), []*KVPairMutation{{
			Op: OpTypePut, Key: MigrationConfigurationIndexKey(), Value: append(bytes.Clone(marker), '{'),
		}}, nil, ts, ts, 11))
		before, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Error(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 12))
		after, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}

func activatedPartitionSnapshotSource(t *testing.T, format string) (MVCCStore, []PebbleStoreOption) {
	t.Helper()
	if format == "mvcc" {
		return NewMVCCStore(), nil
	}
	if format == "native" || format == "sst" {
		source, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, source.Close()) })
		return source, nil
	}
	fixture := newEncryptedStoreFixture(t, 7)
	ps, ok := fixture.mvcc.(*pebbleStore)
	require.True(t, ok)
	ps.sstIngestSnapshots = format == "encrypted-sst"
	return fixture.mvcc, []PebbleStoreOption{WithEncryption(fixture.cipher, encryption.NewDeterministicNonceFactory(0xABCD, 2),
		func() (uint32, bool) { return fixture.keyID, true })}
}

func TestOwnershipPartitionActivatedNamespaceSnapshotReopen(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst", "encrypted-native", "encrypted-sst"} {
		t.Run(format, func(t *testing.T) {
			source, options := activatedPartitionSnapshotSource(t, format)
			_, tx := partitionFixture(t, source)
			marker := applyPartitionNamespaceMarker(t, source, 3, 10)
			require.NoError(t, partitionStore(t, source).ApplyMigrationOwnershipPartition(t.Context(), tx, 11))
			expected, err := admissionReader(t, source).MigrationServingAdmissionState(t.Context())
			require.NoError(t, err)
			raw := externalTestSnapshot(t, source)
			dir := t.TempDir()
			destination, err := NewPebbleStore(dir, options...)
			require.NoError(t, err)
			require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 1, 0))
			before := externalTestSnapshot(t, destination)
			require.ErrorIs(t, externalTestRestore(t, destination, raw), ErrExternalSnapshotMigrationState)
			require.Equal(t, before, externalTestSnapshot(t, destination))
			require.NoError(t, destination.Restore(bytes.NewReader(raw)))
			require.NoError(t, destination.Close())
			destination, err = NewPebbleStore(dir, options...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, destination.Close()) })
			actual, err := admissionReader(t, destination).MigrationServingAdmissionState(t.Context())
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			value, err := destination.GetAt(t.Context(), MigrationConfigurationIndexKey(), ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, marker, value)
			configuration, err := partitionConfiguration(value, nil)
			require.NoError(t, err)
			require.Equal(t, uint64(3), configuration)
			p := actual.Partitions.Partitions[0]
			ref, err := MigrationOwnershipPartitionReferenceFor(p)
			require.NoError(t, err)
			refresh := tx
			refresh.Operation, refresh.OperationID, refresh.CatalogVersion = MigrationOwnershipPartitionRefresh, "restored/activated/refresh", 13
			refresh.Before, refresh.After = []MigrationOwnershipPartitionReference{ref}, []MigrationOwnershipPartition{p}
			finalizePartitionTransition(t, &refresh)
			require.NoError(t, partitionStore(t, destination).ApplyMigrationOwnershipPartition(t.Context(), refresh, 12))
		})
	}
}
