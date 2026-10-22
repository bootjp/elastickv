package kv

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func retireServingSource(t *testing.T, st store.MVCCStore) {
	t.Helper()
	writer, ok := st.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), store.TargetStagedReadinessState{
		JobID: 7, MigrationJobID: 7, Armed: true, SourceWriteFence: true, SourceReadFence: true,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 10,
		MinWriteTSExclusive: 100, RetentionPinTS: 1,
	}, 8))
	proofs, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), store.MigrationOwnershipState{
		JobID: 7, LocalGroupID: 1, OwnerGroupID: 2, CatalogVersion: 10, ConfigurationIndex: 3,
		MinWriteTSExclusive: 100, Role: store.MigrationNonOwner, RouteStart: []byte("m"), RouteEnd: []byte("z"),
	}, 9))
}

func staleServingEngine(t *testing.T, version uint64) *distribution.Engine {
	t.Helper()
	engine := distribution.NewEngine()
	applyComposed1Snapshot(t, engine, version, []distribution.RouteDescriptor{{
		RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 1, State: distribution.RouteStateActive,
	}})
	return engine
}

func TestMigrationServingJoinedVoterRejectsStaleCatalogReadsAndApply(t *testing.T) {
	source := store.NewMVCCStore()
	require.NoError(t, source.PutAt(t.Context(), []byte("n"), []byte("old-source"), 90, 0))
	retireServingSource(t, source)
	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	for _, version := range []uint64{9, 10, 11} {
		t.Run(strconv.FormatUint(version, 10), func(t *testing.T) {
			joined, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, joined.Close()) })
			require.NoError(t, joined.Restore(bytes.NewReader(raw.Bytes())))
			engine := staleServingEngine(t, version)
			shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: joined}})
			_, err = shards.GetAt(t.Context(), []byte("n"), 200)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			_, err = shards.GetGroupAt(t.Context(), 1, []byte("n"), 200)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			_, _, err = shards.LatestCommitTSGroupWithReadFence(t.Context(), []byte("n"), 1, 0)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			_, _, err = shards.VersionExistsAtOrBeforeGroupWithReadFence(t.Context(), []byte("n"), 1, 200, 0)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			_, _, err = shards.VersionsExistAtOrBeforeGroupWithReadFence(t.Context(), [][]byte{[]byte("n")}, 1, 200, 0)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			_, err = shards.ScanAt(t.Context(), []byte("m"), []byte("z"), 10, 200)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			_, err = shards.ScanKeysAtWithReadFence(t.Context(), []byte("m"), []byte("z"), 10, 200, 1, 0)
			require.ErrorIs(t, err, ErrRouteCutoverPending)
			fsm, ok := NewKvFSMWithHLC(joined, nil, WithRouteHistory(WrapDistributionEngine(engine), 1)).(*kvFSM)
			require.True(t, ok)
			for _, observed := range []uint64{0, 9, 10, 11} {
				err := fsm.applyRequestErr(t.Context(), &pb.Request{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(observed),
					Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("wrong-owner")}}})
				require.ErrorIs(t, err, ErrRouteCutoverPending)
				require.NoError(t, haltApplyOf(applyErrorResponse(err)))
			}
			require.Equal(t, uint64(100), joined.LastCommitTS())
			value, err := joined.GetAt(t.Context(), []byte("n"), 200)
			require.NoError(t, err)
			require.Equal(t, []byte("old-source"), value)
		})
	}
}

func TestMigrationServingCoalescedRequestsKeepIndependentResults(t *testing.T) {
	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	retireServingSource(t, st)
	engine := staleServingEngine(t, 10)
	fsm, ok := NewKvFSMWithHLC(st, nil, WithRouteHistory(WrapDistributionEngine(engine), 1)).(*kvFSM)
	require.True(t, ok)
	fsm.SetApplyIndex(20)
	cmd, err := marshalRaftCommand([]*pb.Request{
		{Ts: 200, ObservedRouteVersion: EncodeObservedRouteVersion(10), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("good")}}},
		{Ts: 201, ObservedRouteVersion: EncodeObservedRouteVersion(10), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("bad")}}},
		{Ts: 202, ObservedRouteVersion: EncodeObservedRouteVersion(10), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("after")}}},
	})
	require.NoError(t, err)
	response := fsm.Apply(cmd)
	require.NoError(t, haltApplyOf(response))
	results, ok := response.(*fsmApplyResponse)
	require.True(t, ok)
	require.NoError(t, results.results[0])
	require.ErrorIs(t, results.results[1], ErrRouteCutoverPending)
	require.NoError(t, results.results[2])
	value, err := st.GetAt(t.Context(), []byte("c"), 202)
	require.NoError(t, err)
	require.Equal(t, []byte("after"), value)
}

func TestMigrationServingProofRejectsMixedAndFutureVersions(t *testing.T) {
	states := []store.MigrationOwnershipState{{JobID: 8, LocalGroupID: 1, OwnerGroupID: 1, Role: store.MigrationOwner,
		CatalogVersion: 12, RouteStart: []byte("m"), RouteEnd: []byte("z"), MinWriteTSExclusive: 100}}
	for _, version := range []uint64{11, 13} {
		_, err := migrationApplyOwnershipProofs(states, []migrationApplyRange{{[]byte("n"), []byte("o")}}, 1, version)
		require.ErrorIs(t, err, ErrRouteCutoverPending)
	}
	_, err := migrationApplyOwnershipProofs(states, []migrationApplyRange{{[]byte("n"), []byte("o")}, {[]byte("b"), []byte("c")}}, 1, 12)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	_, err = migrationApplyOwnershipProofs(states, []migrationApplyRange{{[]byte("a"), []byte("z")}}, 1, 12)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	proofs, err := migrationApplyOwnershipProofs(states, []migrationApplyRange{{[]byte("n"), []byte("o")}}, 1, 12)
	require.NoError(t, err)
	require.Len(t, proofs, 1)
	request := &pb.Request{IsTxn: true, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b")})},
		{Op: pb.Op_PUT, Key: []byte("n")},
	}}
	ranges, err := migrationServingFootprint(request)
	require.NoError(t, err)
	_, err = migrationApplyOwnershipProofs(states, ranges, 1, 12)
	require.NoError(t, err, "a remote primary reference does not add a local participant")
	request.Mutations = append(request.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: []byte("b")})
	ranges, err = migrationServingFootprint(request)
	require.NoError(t, err)
	_, err = migrationApplyOwnershipProofs(states, ranges, 1, 12)
	require.ErrorIs(t, err, ErrRouteCutoverPending, "a participating primary footprint cannot bypass mixed admission")
}

func TestMigrationServingMatchingCommitAbortDrain(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			fsm := newComposed1FSM(t, staleServingEngine(t, 9), 1)
			meta := &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("n"), CommitTS: 60, LockTTLms: defaultTxnLockTTLms})}
			mutation := &pb.Mutation{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("prepared")}
			require.NoError(t, fsm.handleTxnRequest(context.Background(), &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 40, Mutations: []*pb.Mutation{meta, mutation}}, 40))
			retireServingSource(t, fsm.store)
			request := &pb.Request{IsTxn: true, Phase: phase, Ts: 40, Mutations: []*pb.Mutation{meta, mutation}}
			_, err := fsm.withMigrationOwnershipEvidence(t.Context(), request)
			require.NoError(t, err, "matching persisted preexisting resolution must pass the new gate")
			request.Ts++
			_, err = fsm.withMigrationOwnershipEvidence(t.Context(), request)
			require.ErrorIs(t, err, ErrRouteCutoverPending, "mismatched resolution cannot use a blanket bypass")
		})
	}
}

func TestMigrationServingScopedHistoryCarriesOwnershipFloor(t *testing.T) {
	fsm := newComposed1FSM(t, staleServingEngine(t, 9), 1)
	history, err := migrationOwnershipHistory([]store.MigrationOwnershipState{{
		JobID: 8, LocalGroupID: 1, OwnerGroupID: 1, CatalogVersion: 12,
		Role: store.MigrationOwner, Staged: true, RouteStart: []byte("m"), RouteEnd: []byte("z"), MinWriteTSExclusive: 200,
	}}, 12)
	require.NoError(t, err)
	scoped := withApplyRouteHistory(t.Context(), history)
	snapshot, ok := fsm.routeHistoryForContext(scoped).Current()
	require.True(t, ok)
	require.Equal(t, uint64(12), snapshot.Version())
	route, ok := snapshot.RouteOf([]byte("n"))
	require.True(t, ok)
	require.Equal(t, uint64(200), route.MinWriteTSExclusive)
	require.Equal(t, uint64(8), route.MigrationJobID)
	require.True(t, route.StagedVisibilityActive)
	current, ok := fsm.routeHistoryForContext(t.Context()).Current()
	require.True(t, ok)
	require.Equal(t, uint64(9), current.Version(), "request-local proof does not mutate the watcher")
}
