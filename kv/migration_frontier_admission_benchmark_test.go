package kv

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// Keep durable setup and the cached-view clone outside the selection timing.
// The shared receipt deliberately has many children, as after a large re-split.
func benchmarkFrontierAdmissionView(b *testing.B, size int) store.MigrationServingState {
	b.Helper()
	f, _, _, legacy := resplitSourceReceiptFixture(b)
	before, err := store.MigrationOwnershipLegacyReferenceFor(legacy)
	require.NoError(b, err)
	tx := store.MigrationOwnershipPartitionTransition{OperationID: "kv/benchmark/frontier",
		Operation: store.MigrationOwnershipPartitionSplit, RootManifestHash: bytes.Repeat([]byte{1}, sha256.Size),
		Generation: legacy.Generation, LocalGroupID: legacy.LocalGroupID, CatalogVersion: legacy.CatalogVersion + 1,
		ConfigurationIndex: legacy.ConfigurationIndex, Before: []store.MigrationOwnershipPartitionReference{before}}
	for i := range size {
		start, end := fmt.Appendf(nil, "m%06d", i), fmt.Appendf(nil, "m%06d", i+1)
		if i == 0 {
			start = legacy.RouteStart
		}
		if i == size-1 {
			end = legacy.RouteEnd
		}
		route := distribution.RouteDescriptor{RouteID: uint64(i) + 4, ParentRouteID: legacy.RouteID,
			Start: start, End: end, GroupID: legacy.OwnerGroupID, MinWriteTSExclusive: legacy.MinWriteTSExclusive}
		encoded, encodeErr := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
		require.NoError(b, encodeErr)
		hash := sha256.Sum256(encoded)
		p := store.MigrationOwnershipPartition{Generation: legacy.Generation,
			LocalGroupID: legacy.LocalGroupID, OwnerGroupID: legacy.OwnerGroupID,
			CatalogVersion: tx.CatalogVersion, ConfigurationIndex: tx.ConfigurationIndex,
			RouteID: route.RouteID, ParentRouteID: route.ParentRouteID, RouteStart: start, RouteEnd: end,
			DescriptorHash: hash[:], Role: legacy.Role, Staged: legacy.Staged, MigrationJobID: legacy.JobID,
			MinWriteTSExclusive: legacy.MinWriteTSExclusive, WriteFloorKnown: true,
			SourceTrackerVersion: legacy.SourceTrackerVersion, SourceAppliedIndex: legacy.SourceAppliedIndex}
		for _, receipt := range legacy.Receipts {
			p.Origins = append(p.Origins, store.MigrationOwnershipOperationReference{JobID: legacy.JobID,
				Operation: receipt.Operation, CatalogVersion: receipt.CatalogVersion,
				ConfigurationIndex: receipt.ConfigurationIndex, AppliedIndex: receipt.AppliedIndex,
				DescriptorHash: receipt.DescriptorHash, Staged: receipt.Staged})
		}
		tx.After = append(tx.After, p)
	}
	for i := range tx.After {
		tx.After[i].PartitionID, err = store.MigrationOwnershipPartitionIDFor(tx, i)
		require.NoError(b, err)
	}
	writer, ok := f.store.(store.MigrationOwnershipPartitionStore)
	require.True(b, ok)
	require.NoError(b, writer.ApplyMigrationOwnershipPartition(b.Context(), tx, 12))
	reader, ok := f.store.(store.MigrationServingAdmissionReader)
	require.True(b, ok)
	view, err := reader.MigrationServingAdmissionState(b.Context())
	require.NoError(b, err)
	require.Empty(b, view.Ownership)
	require.Len(b, view.Partitions.Partitions, size)
	return view
}

func BenchmarkMigrationFrontierAdmission(b *testing.B) {
	for _, size := range []int{16, 128, 512} {
		b.Run(fmt.Sprintf("partitions=%d", size), func(b *testing.B) {
			view := benchmarkFrontierAdmissionView(b, size)
			point := view.Partitions.Partitions[size/2].RouteStart
			for _, tc := range []struct {
				name         string
				rangeToProve migrationApplyRange
				want         int
			}{
				{name: "selected-point", rangeToProve: migrationApplyRange{point, nextScanCursor(point)}, want: 1},
				{name: "full-scan", rangeToProve: migrationApplyRange{view.Partitions.Partitions[0].RouteStart, view.Partitions.Partitions[size-1].RouteEnd}, want: size},
			} {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						proofs, err := migrationAdmissionRangeProofs(view, tc.rangeToProve)
						if err != nil || len(proofs) != tc.want {
							b.Fatalf("admission selected %d proofs, want %d: %v", len(proofs), tc.want, err)
						}
					}
				})
			}
		})
	}
}
