package store

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func admissionReader(t *testing.T, st MVCCStore) MigrationServingAdmissionReader {
	t.Helper()
	reader, ok := st.(MigrationServingAdmissionReader)
	require.True(t, ok)
	return reader
}

func servingSummary(t *testing.T, st MVCCStore) MigrationServingSummary {
	t.Helper()
	reader, ok := st.(MigrationServingSummaryReader)
	require.True(t, ok)
	summary, err := reader.MigrationServingSummary(t.Context())
	require.NoError(t, err)
	return summary
}

func refreshAdmissionPartition(t *testing.T, st MVCCStore, version uint64) {
	t.Helper()
	full, err := ownershipStore(t, st).MigrationServingState(t.Context())
	require.NoError(t, err)
	p := full.Partitions.Partitions[0]
	ref, err := MigrationOwnershipPartitionReferenceFor(p)
	require.NoError(t, err)
	tx := MigrationOwnershipPartitionTransition{OperationID: "admission/refresh/" + strconv.FormatUint(version, 10),
		Operation: MigrationOwnershipPartitionRefresh, RootManifestHash: bytes.Repeat([]byte{4}, 32),
		Generation: p.Generation, LocalGroupID: p.LocalGroupID, CatalogVersion: version,
		ConfigurationIndex: p.ConfigurationIndex, Before: []MigrationOwnershipPartitionReference{ref},
		After: []MigrationOwnershipPartition{p}}
	finalizePartitionTransition(t, &tx)
	require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, version))
}

func TestMigrationServingAdmissionCompactsHistoryAndOwnsSlices(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		reader := admissionReader(t, st)
		require.Equal(t, MigrationServingSummary{}, servingSummary(t, st))
		_, tx := partitionFixture(t, st)
		legacy, err := reader.MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Len(t, legacy.Ownership, 1)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		for version := uint64(13); version < 33; version++ {
			refreshAdmissionPartition(t, st, version)
		}
		full, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Len(t, full.Partitions.Receipts, 21)
		view, err := reader.MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Empty(t, view.Ownership, "adopted legacy job must not obscure children")
		require.Len(t, view.Partitions.Receipts, 2, "cost depends on active support, not immutable history")
		require.Equal(t, full.Partitions.Partitions, view.Partitions.Partitions)
		for _, p := range view.Partitions.Partitions {
			require.True(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, p))
		}
		require.Equal(t, MigrationServingSummary{CatalogVersionFloor: 32, Present: true}, servingSummary(t, st))
		view.Partitions.Partitions[0].DescriptorHash[0]++
		view.Partitions.Receipts[0].Transition.After[0].Origins[0].DescriptorHash[0]++
		after, err := reader.MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, full.Partitions.Partitions, after.Partitions.Partitions)
		require.True(t, MigrationOwnershipPartitionEvidenceValid(after.Partitions, after.Partitions.Partitions[0]))
	})
}

func TestMigrationServingAdmissionUnknownLegacyAndReceiptOnlyRestore(t *testing.T) {
	for _, receiptOnly := range []bool{false, true} {
		source := NewMVCCStore()
		_, tx := partitionFixture(t, source)
		require.NoError(t, partitionStore(t, source).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		ms, ok := source.(*mvccStore)
		require.True(t, ok)
		if receiptOnly {
			ms.migrationOwnership = nil
			ms.migrationPartitions.Partitions = nil
		} else {
			unknown := ms.migrationOwnership[0]
			unknown.JobID, unknown.Receipts = 99, nil
			ms.migrationOwnership = append(ms.migrationOwnership, unknown)
		}
		raw := externalTestSnapshot(t, source)
		runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
			require.NoError(t, st.Restore(bytes.NewReader(raw)))
			view, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
			require.NoError(t, err)
			if receiptOnly {
				require.Empty(t, view.Partitions.Partitions)
				require.Empty(t, view.Partitions.Receipts)
			} else {
				require.Len(t, view.Ownership, 1)
				require.Equal(t, uint64(99), view.Ownership[0].JobID)
				require.False(t, MigrationOwnershipEvidenceValid(view.Ownership[0]))
			}
			require.True(t, servingSummary(t, st).Present)
			clean := externalTestSnapshot(t, NewMVCCStore())
			require.NoError(t, st.Restore(bytes.NewReader(clean)))
			require.Equal(t, MigrationServingSummary{}, servingSummary(t, st))
			view, err = admissionReader(t, st).MigrationServingAdmissionState(t.Context())
			require.NoError(t, err)
			require.Empty(t, view.Ownership)
			require.Empty(t, view.Partitions.Partitions)
		})
	}
}

func TestMigrationServingAdmissionDoesNotAcquireDatabaseOrApplyLock(t *testing.T) {
	for _, migrated := range []bool{false, true} {
		st, err := NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		if migrated {
			_, tx := partitionFixture(t, st)
			require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		}
		ps, ok := st.(*pebbleStore)
		require.True(t, ok)
		func() {
			ps.applyMu.Lock()
			defer ps.applyMu.Unlock()
			ps.dbMu.Lock()
			defer ps.dbMu.Unlock()
			done := make(chan error, 1)
			go func() {
				_, readErr := ps.MigrationServingAdmissionState(t.Context())
				if readErr == nil {
					_, readErr = ps.MigrationServingSummary(t.Context())
				}
				done <- readErr
			}()
			select {
			case readErr := <-done:
				require.NoError(t, readErr)
			case <-time.After(5 * time.Second):
				t.Fatal("admission reader acquired DB/apply lock")
			}
		}()
	}
}

func TestMigrationServingAdmissionConcurrentPublication(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		wg.Go(func() {
			reader := admissionReader(t, st)
			for ctx.Err() == nil {
				view, err := reader.MigrationServingAdmissionState(ctx)
				if ctx.Err() != nil {
					return
				}
				require.NoError(t, err)
				for _, p := range view.Partitions.Partitions {
					require.True(t, MigrationOwnershipPartitionEvidenceValid(view.Partitions, p))
				}
			}
		})
		for version := uint64(13); version < 23; version++ {
			refreshAdmissionPartition(t, st, version)
		}
		cancel()
		wg.Wait()
	})
}
