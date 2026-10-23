package kv

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// This is the negative half of the runtime re-split contract. The adapter
// acceptance owns real SplitRange/runner/partition progress; no child authority
// is synthesized here to turn a newer watcher into a committed proof.
func resplitSourceReceiptFixture(t *testing.T) (*kvFSM, *distribution.CatalogStore, *distribution.Engine, store.MigrationOwnershipState) {
	t.Helper()
	root := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(root, distribution.WithCatalogRouteDescriptorV2Writes(true))
	parent := distribution.RouteDescriptor{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"),
		GroupID: 2, MinWriteTSExclusive: 100}
	snapshot, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{parent})
	require.NoError(t, err)
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(snapshot))
	local, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	fsm, ok := NewKvFSMWithHLC(local, nil, WithRouteHistory(WrapDistributionEngine(engine), 2)).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, fsm.RecordAppliedConfiguration(9))
	guard, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7,
		Armed: true, TrackWrites: true, RouteStart: parent.Start, RouteEnd: parent.End,
		ExpectedCutoverVersion: snapshot.Version, MinWriteTsExclusive: 100, RetentionPinTs: 1})
	require.NoError(t, err)
	fsm.SetApplyIndex(10)
	require.Nil(t, fsm.Apply(guard))
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(parent, true)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	var generation [16]byte
	_, err = rand.Read(generation[:])
	require.NoError(t, err)
	acquire, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{JobId: 7,
		Mode:                      pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_SOURCE_OWNERSHIP_ACQUIRE,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9,
		Ownership: &pb.MigrationOwnershipProof{Generation: hex.EncodeToString(generation[:]),
			LocalGroupId: 2, OwnerGroupId: 2, CatalogVersion: snapshot.Version, RouteId: parent.RouteID,
			ParentRouteId: parent.ParentRouteID, RouteStart: parent.Start, RouteEnd: parent.End,
			WriteFloorKnown: true, MinWriteTsExclusive: 100, SourceTrackerVersion: snapshot.Version,
			DescriptorHash: digest[:], Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER}})
	require.NoError(t, err)
	fsm.SetApplyIndex(11)
	result, ok := fsm.Apply(acquire).(store.CleanupVersionsResult)
	require.True(t, ok)
	require.True(t, result.Done)
	reader, ok := local.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Len(t, view.Ownership, 1)
	proof := view.Ownership[0]
	require.True(t, store.MigrationOwnershipEvidenceValid(proof))
	require.NotEmpty(t, proof.Receipts)
	require.EqualValues(t, 11, proof.SourceAppliedIndex)
	return fsm, catalog, engine, proof
}

func TestMigrationResplitAcceptanceNewMirrorCannotReplacePartitionReceipt(t *testing.T) {
	for _, rightOwner := range []uint64{2, 3} {
		t.Run(map[uint64]string{2: "same-owner children", 3: "moving-right child"}[rightOwner], func(t *testing.T) {
			fsm, catalog, engine, proof := resplitSourceReceiptFixture(t)
			children := []distribution.RouteDescriptor{
				{RouteID: 4, ParentRouteID: 3, Start: []byte("m"), End: []byte("t"), GroupID: 2, MinWriteTSExclusive: 100},
				{RouteID: 5, ParentRouteID: 3, Start: []byte("t"), End: []byte("z"), GroupID: rightOwner, MinWriteTSExclusive: 100},
			}
			snapshot, err := catalog.Save(t.Context(), proof.CatalogVersion, children)
			require.NoError(t, err)
			require.NoError(t, engine.ApplySnapshot(snapshot))
			for _, key := range []string{"n", "y"} {
				request := &pb.Request{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(snapshot.Version),
					Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("unproved-new-C")}}}
				require.ErrorIs(t, CheckMigrationRequest(t.Context(), fsm.store, 2, request), ErrRouteCutoverPending)
				require.ErrorIs(t, applyFSMRequest(t, fsm, request), ErrRouteCutoverPending)
				_, err = fsm.store.GetAt(t.Context(), []byte(key), ^uint64(0))
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		})
	}
}

func TestMigrationResplitAcceptanceLoggedParentEntryKeepsReceiptAndFloor(t *testing.T) {
	fsm, catalog, engine, proof := resplitSourceReceiptFixture(t)
	snapshot, err := catalog.Save(t.Context(), proof.CatalogVersion, []distribution.RouteDescriptor{
		{RouteID: 4, ParentRouteID: 3, Start: []byte("m"), End: []byte("t"), GroupID: 2, MinWriteTSExclusive: 100},
		{RouteID: 5, ParentRouteID: 3, Start: []byte("t"), End: []byte("z"), GroupID: 2, MinWriteTSExclusive: 100},
	})
	require.NoError(t, err)
	require.NoError(t, engine.ApplySnapshot(snapshot))
	for _, key := range []string{"n", "y"} {
		request := &pb.Request{Ts: 100, ObservedRouteVersion: EncodeObservedRouteVersion(proof.CatalogVersion),
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: []byte("logged-before-split")}}}
		require.ErrorIs(t, applyFSMRequest(t, fsm, request), ErrRouteWriteTimestampTooLow)
		request.Ts = 200
		require.NoError(t, applyFSMRequest(t, fsm, request), "apply uses immutable logged parent evidence, not the newer mirror")
		value, getErr := fsm.store.GetAt(t.Context(), []byte(key), 200)
		require.NoError(t, getErr)
		require.Equal(t, []byte("logged-before-split"), value)
	}
}
