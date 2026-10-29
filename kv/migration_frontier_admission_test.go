package kv

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func frontierAdmissionSplitFixture(t *testing.T) (*kvFSM, *distribution.Engine, store.MigrationServingState) {
	t.Helper()
	f, catalog, engine, legacy := resplitSourceReceiptFixture(t)
	children := []distribution.RouteDescriptor{
		{RouteID: 4, ParentRouteID: 3, Start: []byte("m"), End: []byte("t"), GroupID: 2, MinWriteTSExclusive: 100},
		{RouteID: 5, ParentRouteID: 3, Start: []byte("t"), End: []byte("z"), GroupID: 2, MinWriteTSExclusive: 100},
	}
	snapshot, err := catalog.Save(t.Context(), legacy.CatalogVersion, children)
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(snapshot))
	view := frontierAdmissionSplitLegacy(t, f, legacy, children, snapshot.Version)
	return f, engine, view
}

func frontierAdmissionSplitLegacy(t *testing.T, f *kvFSM, legacy store.MigrationOwnershipState, children []distribution.RouteDescriptor, version uint64) store.MigrationServingState {
	t.Helper()
	before, err := store.MigrationOwnershipLegacyReferenceFor(legacy)
	require.NoError(t, err)
	tx := store.MigrationOwnershipPartitionTransition{OperationID: "kv/frontier/split", Operation: store.MigrationOwnershipPartitionSplit,
		RootManifestHash: bytes.Repeat([]byte{1}, sha256.Size), Generation: legacy.Generation, LocalGroupID: legacy.LocalGroupID,
		CatalogVersion: version, ConfigurationIndex: legacy.ConfigurationIndex, Before: []store.MigrationOwnershipPartitionReference{before}}
	for _, child := range children {
		encoded, encodeErr := distribution.EncodeRouteDescriptorForCatalogWrite(child, true)
		require.NoError(t, encodeErr)
		hash := sha256.Sum256(encoded)
		p := store.MigrationOwnershipPartition{Generation: legacy.Generation, LocalGroupID: legacy.LocalGroupID, OwnerGroupID: legacy.OwnerGroupID,
			CatalogVersion: version, ConfigurationIndex: legacy.ConfigurationIndex, RouteID: child.RouteID, ParentRouteID: child.ParentRouteID,
			RouteStart: child.Start, RouteEnd: child.End, DescriptorHash: hash[:], Role: legacy.Role, Staged: legacy.Staged,
			MinWriteTSExclusive: legacy.MinWriteTSExclusive, WriteFloorKnown: true, MigrationJobID: legacy.JobID,
			SourceTrackerVersion: legacy.SourceTrackerVersion, SourceAppliedIndex: legacy.SourceAppliedIndex}
		for _, receipt := range legacy.Receipts {
			p.Origins = append(p.Origins, store.MigrationOwnershipOperationReference{JobID: legacy.JobID,
				Operation: receipt.Operation, CatalogVersion: receipt.CatalogVersion, ConfigurationIndex: receipt.ConfigurationIndex,
				AppliedIndex: receipt.AppliedIndex, DescriptorHash: receipt.DescriptorHash, Staged: receipt.Staged})
		}
		tx.After = append(tx.After, p)
	}
	for i := range tx.After {
		id, idErr := store.MigrationOwnershipPartitionIDFor(tx, i)
		require.NoError(t, idErr)
		tx.After[i].PartitionID = id
	}
	writer, ok := f.store.(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	require.NoError(t, writer.ApplyMigrationOwnershipPartition(t.Context(), tx, 12))
	reader, ok := f.store.(store.MigrationServingAdmissionReader)
	require.True(t, ok)
	view, err := reader.MigrationServingAdmissionState(t.Context())
	require.NoError(t, err)
	require.Empty(t, view.Ownership, "adopted parent is history, not concurrent serving authority")
	require.Len(t, view.Partitions.Partitions, 2)
	return view
}

func TestMigrationFrontierAdmissionSameJobChildrenKeepIndependentAuthority(t *testing.T) {
	f, engine, _ := frontierAdmissionSplitFixture(t)
	request := &pb.Request{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(engine.Version()),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("left")},
			{Op: pb.Op_PUT, Key: []byte("y"), Value: []byte("right")}}}
	require.NoError(t, CheckMigrationRequest(t.Context(), f.store, 2, request))
	f.SetApplyIndex(13)
	require.NoError(t, applyFSMRequest(t, f, request))
	shards := NewShardStore(engine, map[uint64]*ShardGroup{2: {Store: f.store}})
	for _, key := range []string{"n", "y"} {
		value, err := shards.GetAt(t.Context(), []byte(key), 200)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"n": "left", "y": "right"}[key], string(value))
	}
	rows, err := shards.ScanAt(t.Context(), []byte("m"), []byte("z"), 10, 200)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	request.Ts = 100
	require.ErrorIs(t, applyFSMRequest(t, f, request), ErrRouteWriteTimestampTooLow)
	request.Ts, request.ObservedRouteVersion = 201, EncodeObservedRouteVersion(engine.Version()-1)
	require.ErrorIs(t, CheckMigrationRequest(t.Context(), f.store, 2, request), ErrRouteCutoverPending)
	require.ErrorIs(t, applyFSMRequest(t, f, request), ErrRouteCutoverPending)
}

func TestMigrationFrontierAdmissionRequiresCompleteUnion(t *testing.T) {
	f, engine, view := frontierAdmissionSplitFixture(t)
	proofs, err := migrationAdmissionRangeProofs(view, migrationApplyRange{[]byte("m"), []byte("z")})
	require.NoError(t, err)
	require.Len(t, proofs, 2)
	history, err := migrationAdmissionHistory(proofs, engine.Version())
	require.NoError(t, err)
	snapshot, ok := history.Current()
	require.True(t, ok)
	for _, key := range []string{"n", "y"} {
		route, found := snapshot.RouteOf([]byte(key))
		require.True(t, found, "same-job children must not collapse during history construction")
		require.EqualValues(t, 100, route.MinWriteTSExclusive)
	}
	view.Partitions.Partitions = view.Partitions.Partitions[:1]
	_, err = migrationAdmissionRangeProofs(view, migrationApplyRange{[]byte("m"), []byte("z")})
	require.ErrorIs(t, err, ErrRouteCutoverPending, "historical sibling output cannot fill an inactive frontier hole")
	_, err = migrationAdmissionOwnershipProofs(view, []migrationApplyRange{{[]byte("n"), []byte("o")}, {[]byte("y"), []byte("z")}}, 2, engine.Version())
	require.ErrorIs(t, err, ErrRouteCutoverPending, "one covered mutation cannot authorize an uncovered mutation")
	_, err = migrationAdmissionOwnershipProofs(view, []migrationApplyRange{{[]byte("n"), []byte("o")}}, 3, engine.Version())
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	shards := NewShardStore(engine, map[uint64]*ShardGroup{2: {Store: f.store}})
	_, err = shards.GetAtWithReadFence(t.Context(), []byte("n"), 200, 2, engine.Version()-1)
	require.ErrorIs(t, err, ErrRouteCutoverPending, "a newer watcher does not upgrade the caller's stale fence")
}

func TestMigrationFrontierAdmissionSourceAndStagedBoundaries(t *testing.T) {
	f, engine, _ := frontierAdmissionSplitFixture(t)
	shards := NewShardStore(engine, map[uint64]*ShardGroup{2: {Store: f.store}})
	applyComposed1Snapshot(t, engine, engine.Version()+1, []distribution.RouteDescriptor{
		{RouteID: 4, Start: []byte("m"), End: []byte("t"), GroupID: 2, MinWriteTSExclusive: 99},
		{RouteID: 5, Start: []byte("t"), End: []byte("z"), GroupID: 2, MinWriteTSExclusive: 100,
			MigrationJobID: 99, StagedVisibilityActive: true},
	})
	for _, key := range []string{"n", "y"} {
		_, err := shards.GetAt(t.Context(), []byte(key), 200)
		require.ErrorIs(t, err, ErrRouteCutoverPending, "floor and lineage cannot be borrowed from a different boundary")
	}
	writer, ok := f.store.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), store.TargetStagedReadinessState{
		JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true, SourceWriteFence: true, SourceReadFence: true,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1, MinWriteTSExclusive: 100, RetentionPinTS: 1,
	}, 14))
	request := &pb.Request{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(2),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("fenced")}}}
	require.ErrorIs(t, CheckMigrationRequest(t.Context(), f.store, 2, request), ErrRouteCutoverPending)
	f.SetApplyIndex(15)
	require.ErrorIs(t, applyFSMRequest(t, f, request), ErrRouteCutoverPending)
}

func TestMigrationFrontierAdmissionPartialRefreshDoesNotUpgradeSibling(t *testing.T) {
	f, engine, view := frontierAdmissionSplitFixture(t)
	left := view.Partitions.Partitions[0]
	ref, err := store.MigrationOwnershipPartitionReferenceFor(left)
	require.NoError(t, err)
	left.CatalogVersion++
	left.ReceiptID, left.AppliedIndex = nil, 0
	refresh := store.MigrationOwnershipPartitionTransition{OperationID: "kv/frontier/refresh-left", Operation: store.MigrationOwnershipPartitionRefresh,
		RootManifestHash: bytes.Repeat([]byte{2}, sha256.Size), Generation: left.Generation, LocalGroupID: 2,
		CatalogVersion: left.CatalogVersion, ConfigurationIndex: 9, Before: []store.MigrationOwnershipPartitionReference{ref},
		After: []store.MigrationOwnershipPartition{left}}
	writer, ok := f.store.(store.MigrationOwnershipPartitionStore)
	require.True(t, ok)
	require.NoError(t, writer.ApplyMigrationOwnershipPartition(t.Context(), refresh, 14))
	request := &pb.Request{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(left.CatalogVersion),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("left")}}}
	require.NoError(t, CheckMigrationRequest(t.Context(), f.store, 2, request))
	f.SetApplyIndex(15)
	require.NoError(t, applyFSMRequest(t, f, request), "a stale watcher cannot override durable child evidence")
	request.Mutations = append(request.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte("y"), Value: []byte("not-refreshed")})
	require.ErrorIs(t, CheckMigrationRequest(t.Context(), f.store, 2, request), ErrRouteCutoverPending)
	require.ErrorIs(t, applyFSMRequest(t, f, request), ErrRouteCutoverPending)
	request.ObservedRouteVersion = EncodeObservedRouteVersion(engine.Version())
	require.ErrorIs(t, CheckMigrationRequest(t.Context(), f.store, 2, request), ErrRouteCutoverPending, "old active attestation is now history only")
	_, err = f.store.GetAt(t.Context(), []byte("y"), 200)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestMigrationFrontierAdmissionNonOwnerChildrenNeverGrant(t *testing.T) {
	st := store.NewMVCCStore()
	retireServingSource(t, st)
	engine := staleServingEngine(t, 11)
	f, ok := NewKvFSMWithHLC(st, nil, WithRouteHistory(WrapDistributionEngine(engine), 1)).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, f.RecordAppliedConfiguration(3))
	reader, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	full, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	view := frontierAdmissionSplitLegacy(t, f, full.Ownership[0], []distribution.RouteDescriptor{
		{RouteID: 4, ParentRouteID: 3, Start: []byte("m"), End: []byte("t"), GroupID: 2, MinWriteTSExclusive: 100},
		{RouteID: 5, ParentRouteID: 3, Start: []byte("t"), End: []byte("z"), GroupID: 2, MinWriteTSExclusive: 100},
	}, 11)
	for _, p := range view.Partitions.Partitions {
		require.Equal(t, store.MigrationNonOwner, p.Role)
		require.True(t, store.MigrationOwnershipPartitionEvidenceValid(view.Partitions, p))
	}
	require.NoError(t, st.PutAt(t.Context(), []byte("n"), []byte("old-source"), 90, 0))
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: st}})
	_, err = shards.GetAt(t.Context(), []byte("n"), 200)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, err = shards.ScanAt(t.Context(), []byte("m"), []byte("z"), 10, 200)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, _, err = shards.VersionExistsAtOrBeforeGroupWithReadFence(t.Context(), []byte("n"), 1, 200, 0)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	request := &pb.Request{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(11),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("y"), Value: []byte("denied")}}}
	require.ErrorIs(t, CheckMigrationRequest(t.Context(), st, 1, request), ErrRouteCutoverPending)
	f.SetApplyIndex(13)
	require.ErrorIs(t, applyFSMRequest(t, f, request), ErrRouteCutoverPending)
}

func TestMigrationFrontierAdmissionStaleFenceRejectedBeforeProxy(t *testing.T) {
	f, engine, _ := frontierAdmissionSplitFixture(t)
	shards := NewShardStore(engine, map[uint64]*ShardGroup{
		2: {Store: f.store, Engine: newStubFollowerEngine("127.0.0.1:1")},
	})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })
	_, err := shards.GetAtWithReadFence(t.Context(), []byte("n"), 200, 0, engine.Version()-1)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, _, err = shards.LatestCommitTSWithReadFence(t.Context(), []byte("n"), engine.Version()-1)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, _, err = shards.LatestCommitTSGroupWithReadFence(t.Context(), []byte("n"), 2, engine.Version()-1)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, _, err = shards.VersionExistsAtOrBeforeGroupWithReadFence(t.Context(), []byte("n"), 2, 200, engine.Version()-1)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, _, err = shards.VersionsExistAtOrBeforeGroupWithReadFence(t.Context(), [][]byte{[]byte("n")}, 2, 200, engine.Version()-1)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, err = shards.ScanAtWithReadFence(t.Context(), []byte("m"), []byte("t"), 10, 200, false, 0, engine.Version()-1, nil, nil)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
}

func TestMigrationFrontierAdmissionUnknownLegacyCannotBorrowNewerPartition(t *testing.T) {
	_, engine, view := frontierAdmissionSplitFixture(t)
	view.Ownership = []store.MigrationOwnershipState{{JobID: 7, LocalGroupID: 2, OwnerGroupID: 2,
		CatalogVersion: engine.Version() - 1, ConfigurationIndex: 9, AppliedIndex: 11, Role: store.MigrationOwner,
		RouteStart: []byte("m"), RouteEnd: []byte("t"), MinWriteTSExclusive: 100}}
	_, err := migrationAdmissionOwnershipProofs(view, []migrationApplyRange{{[]byte("n"), []byte("o")}}, 2, engine.Version())
	require.ErrorIs(t, err, ErrRouteCutoverPending, "receipt-less legacy remains an interval barrier even beside newer valid frontier evidence")
	_, err = migrationAdmissionOwnershipProofs(view, []migrationApplyRange{{[]byte("y"), []byte("z")}}, 2, engine.Version())
	require.NoError(t, err, "unknown legacy must not block an unaffected sibling")
}
