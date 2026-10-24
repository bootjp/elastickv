package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestPartitionCandidatesGuardedNewJobIsNotDuplicated(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	children := h.split(t, h.parent, 2, 3, "m")
	require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), 7))
	right := children[1]
	hash, err := distribution.OwnershipPartitionDescriptorHash(right)
	require.NoError(t, err)
	guard := store.TargetStagedReadinessState{JobID: 12, MigrationJobID: 12, Armed: true, TrackWrites: true,
		RouteStart: right.Start, RouteEnd: right.End, ExpectedCutoverVersion: 3, MinWriteTSExclusive: 100, RetentionPinTS: 1}
	guardWriter, ok := h.stores[0].(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, guardWriter.ApplyTargetStagedReadinessAt(t.Context(), guard, 100))
	writer, ok := h.stores[0].(store.MigrationOwnershipStore)
	require.True(t, ok)
	state := store.MigrationOwnershipState{JobID: 12, Generation: "0123456789abcdef0123456789abcdef", LocalGroupID: 7, OwnerGroupID: 7,
		CatalogVersion: 3, ConfigurationIndex: 9, RouteID: right.RouteID, ParentRouteID: right.ParentRouteID, RouteStart: right.Start, RouteEnd: right.End,
		DescriptorHash: hash, Role: store.MigrationOwner, MinWriteTSExclusive: 100, WriteFloorKnown: true, SourceTrackerVersion: 3}
	require.NoError(t, writer.AcquireMigrationOwnership(t.Context(), state, 101))
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Len(t, view.Ownership, 2, "the immutable per-job ledger remains complete")
	frontier := h.frontier(t, 0)
	require.Len(t, frontier.Partitions, 2)
	candidates := migrationPartitionCandidates(view, frontier)
	require.Len(t, candidates, 2, "a guarded job receipt adopts job 12 even without a LegacyJobID Before reference")
	for _, candidate := range candidates {
		require.Nil(t, candidate.legacy)
	}
	invalid := frontier
	invalid.Receipts = append([]store.MigrationOwnershipPartitionReceipt(nil), frontier.Receipts...)
	for i := range invalid.Receipts {
		if invalid.Receipts[i].Transition.Operation == store.MigrationOwnershipPartitionJobOperation {
			invalid.Receipts[i].ReceiptID = []byte("corrupt")
		}
	}
	require.Len(t, migrationPartitionCandidates(view, invalid), 3, "an invalid internal receipt cannot adopt a legacy job")
	unknown := view.Ownership[1]
	unknown.DescriptorHash = nil
	view.Ownership = append(view.Ownership, unknown)
	candidates = migrationPartitionCandidates(view, frontier)
	require.Len(t, candidates, 3, "unknown legacy evidence must remain a refusal barrier")
	require.NotNil(t, candidates[2].legacy)
}
