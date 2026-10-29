package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestOwnershipPartitionSnapshotFormatsAndReopen(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst", "encrypted-native", "encrypted-sst"} {
		t.Run(format, func(t *testing.T) {
			var source MVCCStore
			var options []PebbleStoreOption
			switch format {
			case "mvcc":
				source = NewMVCCStore()
			case "native", "sst":
				var err error
				source, err = NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(format == "sst"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, source.Close()) })
			default:
				fixture := newEncryptedStoreFixture(t, 7)
				source = fixture.mvcc
				ps, ok := source.(*pebbleStore)
				require.True(t, ok)
				ps.sstIngestSnapshots = format == "encrypted-sst"
				options = []PebbleStoreOption{WithEncryption(fixture.cipher, encryption.NewDeterministicNonceFactory(0xABCD, 2), func() (uint32, bool) { return fixture.keyID, true })}
			}
			_, tx := partitionFixture(t, source)
			require.NoError(t, partitionStore(t, source).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
			require.NoError(t, source.PutAt(t.Context(), []byte("user"), []byte("payload"), 20, 0))
			expected, err := ownershipStore(t, source).MigrationServingState(t.Context())
			require.NoError(t, err)
			expectedAdmission, err := admissionReader(t, source).MigrationServingAdmissionState(t.Context())
			require.NoError(t, err)
			raw := externalTestSnapshot(t, source)
			if format == "mvcc" {
				require.Equal(t, mvccSnapshotVersionV7, binary.LittleEndian.Uint32(raw[len(mvccSnapshotMagic):]))
				memory := NewMVCCStore()
				require.NoError(t, memory.Restore(bytes.NewReader(raw)))
				view, readErr := ownershipStore(t, memory).MigrationServingState(t.Context())
				require.NoError(t, readErr)
				require.Equal(t, expected, view)
			}
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
			actual, err := ownershipStore(t, destination).MigrationServingState(t.Context())
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			actualAdmission, err := admissionReader(t, destination).MigrationServingAdmissionState(t.Context())
			require.NoError(t, err)
			require.Equal(t, expectedAdmission, actualAdmission)
			value, err := destination.GetAt(t.Context(), []byte("user"), 20)
			require.NoError(t, err)
			require.Equal(t, []byte("payload"), value)
			// The immutable original receipt remains an idempotency key after
			// every format conversion and reopen.
			require.NoError(t, partitionStore(t, destination).ApplyMigrationOwnershipPartition(t.Context(), tx, 11))
		})
	}
}

func TestOwnershipPartitionExternalRestoreRejectsContainerAlone(t *testing.T) {
	for _, receiptOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "frontier", true: "receipts-only"}[receiptOnly], func(t *testing.T) {
			memory := NewMVCCStore()
			_, tx := partitionFixture(t, memory)
			require.NoError(t, partitionStore(t, memory).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
			ms, ok := memory.(*mvccStore)
			require.True(t, ok)
			ms.migrationOwnership, ms.migrationRetired = nil, nil
			if receiptOnly {
				ms.migrationPartitions.Partitions = nil
			}
			for _, format := range []string{"mvcc", "native", "sst"} {
				raw := externalTestFormat(t, ms, format)
				runMigrationStoreSuite(t, func(t *testing.T, destination MVCCStore) {
					if _, isMemory := destination.(*mvccStore); isMemory && format != "mvcc" {
						return
					}
					require.NoError(t, destination.PutAt(t.Context(), []byte("sentinel"), []byte("unchanged"), 1, 0))
					before := externalTestSnapshot(t, destination)
					err := externalTestRestore(t, destination, raw)
					require.ErrorIs(t, err, ErrExternalSnapshotMigrationState)
					require.Equal(t, before, externalTestSnapshot(t, destination))
				})
			}
		})
	}
}

func TestOwnershipPartitionFailedWALBatchKeepsFrontierReceiptAndIndex(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{DisableWAL: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	st := &pebbleStore{db: db, log: slog.Default(), fsmApplyWriteOpts: pebble.NoSync}
	_, tx := partitionFixture(t, st)
	st.fsmApplyWriteOpts = pebble.Sync
	before, err := st.MigrationServingState(t.Context())
	require.NoError(t, err)
	beforeAdmission, err := st.MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	require.ErrorContains(t, st.ApplyMigrationOwnershipPartition(t.Context(), tx, 10), "WAL disabled")
	after, err := st.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after)
	afterAdmission, err := st.MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	require.Equal(t, beforeAdmission, afterAdmission)
	index, found, err := st.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), index)
	st.fsmApplyWriteOpts = pebble.NoSync
	require.NoError(t, st.ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
	before, err = st.MigrationServingState(t.Context())
	require.NoError(t, err)
	beforeAdmission, err = st.MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	st.fsmApplyWriteOpts = pebble.Sync
	incoming := before.Ownership[0]
	incoming.JobID, incoming.CatalogVersion = 8, 14
	incoming.RouteStart, incoming.RouteEnd = []byte("m"), []byte("z")
	incoming.RouteID, incoming.ParentRouteID, incoming.DescriptorHash = 12, 10, bytes.Repeat([]byte{3}, 32)
	require.ErrorContains(t, st.ClearMigrationStateWithOwnership(t.Context(), incoming, 11), "WAL disabled")
	after, err = st.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after, "failed job cleanup does not retire or publish frontier/receipt")
	afterAdmission, err = st.MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	require.Equal(t, beforeAdmission, afterAdmission)
	retired, err := st.MigrationRetired(context.Background(), 8)
	require.NoError(t, err)
	require.False(t, retired)
}

func TestOwnershipPartitionCorruptionAndNamespaceProtection(t *testing.T) {
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	_, tx := partitionFixture(t, st)
	require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
	_, err = st.ImportVersions(t.Context(), ImportVersionsOptions{JobID: 9, BracketID: 1, BatchSeq: 1, Versions: []MVCCVersion{{
		Key: MigrationOwnershipPartitionMetadataKey(), Value: []byte("forged"), CommitTS: 200,
	}}})
	require.ErrorIs(t, err, ErrInvalidImportVersion)
	require.True(t, isPebbleMetaKey(MigrationOwnershipPartitionMetadataKey()))
	ps, ok := st.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, ps.db.Set(migrationOwnershipPartitionMetaKey, []byte("corrupt"), pebble.Sync))
	_, err = ps.MigrationServingState(t.Context())
	require.ErrorContains(t, err, "invalid ownership partition encoding")
	require.NoError(t, st.Close())
	_, err = NewPebbleStore(dir)
	require.ErrorContains(t, err, "invalid ownership partition encoding")
}

func TestOwnershipPartitionRefusesRawEntryScopeWithoutReenteringOrEffects(t *testing.T) {
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	old, tx := partitionFixture(t, st)
	ps, ok := st.(*pebbleStore)
	require.True(t, ok)
	before, err := ps.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, ps.ApplyRaftEntry(t.Context(), 10, func(ctx context.Context) error {
		view, readErr := ps.MigrationServingState(ctx)
		require.NoError(t, readErr)
		require.Equal(t, before, view)
		require.ErrorIs(t, ps.ApplyMigrationOwnershipPartition(ctx, tx, 10), ErrMigrationOwnership)
		require.ErrorIs(t, ps.AcquireMigrationOwnership(ctx, old, 10), ErrMigrationOwnership)
		require.ErrorIs(t, ps.ClearMigrationStateWithOwnership(ctx, old, 10), ErrMigrationOwnership)
		require.ErrorIs(t, ps.RefreshMigrationOwnership(ctx, old, 10), ErrMigrationOwnership)
		return ErrMigrationOwnership
	}), ErrMigrationOwnership)
	after, err := ps.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after)
	index, found, err := ps.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(9), index)
}
