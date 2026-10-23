package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func splitServingSourceFixture(t *testing.T) (store.MigrationServingState, store.TargetStagedReadinessState) {
	t.Helper()
	st := store.NewMVCCStore()
	guard := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1, MinWriteTSExclusive: 1, RetentionPinTS: 1}
	guards, ok := st.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, guards.ApplyTargetStagedReadinessAt(t.Context(), guard, 10))
	state := store.MigrationOwnershipState{Generation: "0123456789abcdef0123456789abcdef", JobID: 7, LocalGroupID: 1, OwnerGroupID: 1,
		Role: store.MigrationOwner, CatalogVersion: 1, ConfigurationIndex: 9, RouteID: 1, DescriptorHash: bytes.Repeat([]byte{1}, 32),
		RouteStart: []byte("m"), RouteEnd: []byte("z"), WriteFloorKnown: true, SourceTrackerVersion: 1}
	writer, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	require.NoError(t, writer.AcquireMigrationOwnership(t.Context(), state, 11))
	view, err := writer.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.True(t, store.MigrationOwnershipEvidenceValid(view.Ownership[0]))
	return view, guard
}

func TestSplitServingOwnerSourceFenceSelection(t *testing.T) {
	view, guard := splitServingSourceFixture(t)
	p := kv.MigrationPartitionFromLegacy(view.Ownership[0])
	require.True(t, splitPartitionHasServingOwner(view), "an unfenced SOURCE still serves")
	guard.SourceWriteFence, guard.SourceReadFence, guard.ExpectedCutoverVersion = true, true, 2
	view.Readiness = []store.TargetStagedReadinessState{guard}
	require.False(t, splitPartitionHasServingOwner(view), "later durable fence is not a demand for a stale SOURCE refresh")
	for _, tc := range []struct {
		name string
		edit func(*store.TargetStagedReadinessState)
	}{
		{"unarmed", func(g *store.TargetStagedReadinessState) { g.Armed = false }},
		{"wrong job", func(g *store.TargetStagedReadinessState) { g.JobID++ }},
		{"unknown pin", func(g *store.TargetStagedReadinessState) { g.RetentionPinTS = 0 }},
		{"partial coverage", func(g *store.TargetStagedReadinessState) { g.RouteEnd = []byte("t") }},
		{"unknown floor", func(g *store.TargetStagedReadinessState) { g.MinWriteTSExclusive = 0 }},
		{"read fence without write fence", func(g *store.TargetStagedReadinessState) { g.SourceWriteFence = false }},
		{"unsupported guard", func(g *store.TargetStagedReadinessState) { g.TransferSizeGuardVersion = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := guard
			tc.edit(&changed)
			view.Readiness = []store.TargetStagedReadinessState{changed}
			require.True(t, splitPartitionProofServing(view, p), "an invalid or unrelated guard cannot suppress an owner obligation")
		})
	}
	p.SourceTrackerVersion = 0
	require.True(t, splitPartitionProofServing(view, p), "ordinary OWNER is always selected")
	p.Role = store.MigrationNonOwner
	require.False(t, splitPartitionProofServing(view, p))
}

func TestSplitServingOwnersRefreshLegacyCurrentRootAndRequireMembers(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	_, err := h.root.catalog.Save(t.Context(), 1, []distribution.RouteDescriptor{h.parent})
	require.NoError(t, err)
	h.clients[2].old = true
	require.Error(t, h.consumer.reconcileSplitServingOwners(t.Context(), 7))
	h.clients[2].old = false
	require.NoError(t, h.consumer.reconcileSplitServingOwners(t.Context(), 7))
	for _, st := range h.stores {
		reader, ok := st.(store.MigrationOwnershipStore)
		require.True(t, ok)
		view, readErr := reader.MigrationServingState(t.Context())
		require.NoError(t, readErr)
		require.EqualValues(t, 2, view.Ownership[0].CatalogVersion)
	}
}

func TestSplitServingOwnersSkipNegativeOnlyWithoutRemoteEffects(t *testing.T) {
	h := newPartitionRefreshHarness(t, 9)
	h.consumer.ownershipRefresh.root = func(context.Context) (MigrationOwnershipAttestor, error) {
		return nil, errors.New("unrelated negative root must not be queried")
	}
	h.clients[2].old = true
	require.NoError(t, h.consumer.reconcileSplitServingOwners(t.Context(), 9))
	require.Empty(t, h.clients[0].cleanups)
}

func TestSplitServingOwnersProtectedSourceWithoutRemoteEffects(t *testing.T) {
	h := newPartitionRefreshHarness(t, 9)
	for _, st := range h.stores {
		guards, ok := st.(store.MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		guard := store.TargetStagedReadinessState{JobID: 12, MigrationJobID: 12, Armed: true, TrackWrites: true,
			RouteStart: []byte("a"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 2, MinWriteTSExclusive: 100, RetentionPinTS: 1}
		require.NoError(t, guards.ApplyTargetStagedReadinessAt(t.Context(), guard, 11))
		writer, ok := st.(store.MigrationOwnershipStore)
		require.True(t, ok)
		proof := store.MigrationOwnershipState{Generation: "0123456789abcdef0123456789abcdef", JobID: 12, LocalGroupID: 9, OwnerGroupID: 9,
			Role: store.MigrationOwner, CatalogVersion: 2, ConfigurationIndex: 9, RouteID: 1, DescriptorHash: bytes.Repeat([]byte{1}, 32),
			RouteStart: []byte("a"), RouteEnd: []byte("z"), WriteFloorKnown: true, MinWriteTSExclusive: 100, SourceTrackerVersion: 2}
		require.NoError(t, writer.AcquireMigrationOwnership(t.Context(), proof, 12))
		guard.SourceWriteFence, guard.SourceReadFence, guard.ExpectedCutoverVersion = true, true, 3
		require.NoError(t, guards.ApplyTargetStagedReadinessAt(t.Context(), guard, 13))
		view, err := writer.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.False(t, splitPartitionHasServingOwner(view))
	}
	h.consumer.ownershipRefresh.root = func(context.Context) (MigrationOwnershipAttestor, error) {
		return nil, errors.New("protected SOURCE must not query root")
	}
	h.clients[2].old = true
	require.NoError(t, h.consumer.reconcileSplitServingOwners(t.Context(), 9))
	require.Empty(t, h.clients[0].cleanups)
}

func TestSplitServingOwnersUnknownBarrierStaysInOwnerClosure(t *testing.T) {
	h := newPartitionRefreshHarness(t, 7)
	reader, ok := h.stores[0].(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	unknown := view.Ownership[0]
	unknown.JobID, unknown.Generation = 12, ""
	view.Ownership = append(view.Ownership, unknown)
	g := h.consumer.ownershipRefresh.groups[7]
	require.Error(t, h.consumer.refreshSplitServingOwnerClosures(t.Context(), 7, g, view, 9))
	require.Empty(t, partitionOnlyCleanups(h.clients[0]))
}
