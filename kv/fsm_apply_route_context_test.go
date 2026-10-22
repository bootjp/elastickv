package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type immutableApplyRouteHistory struct {
	current  RouteSnapshot
	observed RouteSnapshot
}

func (h immutableApplyRouteHistory) Current() (RouteSnapshot, bool) {
	return h.current, h.current != nil
}

func (h immutableApplyRouteHistory) SnapshotAt(version uint64) (RouteSnapshot, bool) {
	if h.observed != nil && h.observed.Version() == version {
		return h.observed, true
	}
	if h.current != nil && h.current.Version() == version {
		return h.current, true
	}
	return nil, false
}

func applyRouteContextHistory(t *testing.T, version uint64, route distribution.RouteDescriptor) RouteHistory {
	t.Helper()
	e := distribution.NewEngine()
	applyComposed1Snapshot(t, e, version, []distribution.RouteDescriptor{route})
	snapshot, ok := WrapDistributionEngine(e).Current()
	require.True(t, ok)
	return immutableApplyRouteHistory{current: snapshot}
}

func TestApplyRouteHistoryContextIsolation(t *testing.T) {
	t.Parallel()
	base := context.Background()
	legacy := applyRouteContextHistory(t, 1, distribution.RouteDescriptor{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive})
	first := applyRouteContextHistory(t, 2, distribution.RouteDescriptor{RouteID: 2, GroupID: 2, State: distribution.RouteStateActive})
	second := applyRouteContextHistory(t, 3, distribution.RouteDescriptor{RouteID: 3, GroupID: 3, State: distribution.RouteStateActive})
	f := &kvFSM{routes: legacy}
	firstCtx := withApplyRouteHistory(base, first)
	secondCtx := withApplyRouteHistory(base, second)
	require.Equal(t, legacy, f.routeHistoryForContext(base))
	require.Equal(t, first, f.routeHistoryForContext(firstCtx))
	require.Equal(t, second, f.routeHistoryForContext(secondCtx))
	require.Equal(t, second, f.routeHistoryForContext(withApplyRouteHistory(firstCtx, second)))
	require.Equal(t, legacy, f.routes)
}

func applyRouteContextDescriptor() distribution.RouteDescriptor {
	return distribution.RouteDescriptor{
		RouteID: 10, Start: []byte("a"), End: []byte("z"), GroupID: 1,
		State: distribution.RouteStateActive, MinWriteTSExclusive: 100,
		MigrationJobID: 11, StagedVisibilityActive: true,
	}
}

func applyRouteContextWithObserved(t *testing.T, current, observed distribution.RouteDescriptor) RouteHistory {
	t.Helper()
	currentSnap, ok := applyRouteContextHistory(t, 7, current).Current()
	require.True(t, ok)
	observedSnap, ok := applyRouteContextHistory(t, 6, observed).Current()
	require.True(t, ok)
	return immutableApplyRouteHistory{current: currentSnap, observed: observedSnap}
}

func TestApplyRouteContextHelpersIgnoreLocalViews(t *testing.T) {
	t.Parallel()
	route := applyRouteContextDescriptor()
	observed := route
	observed.MinWriteTSExclusive = 50
	ctx := withApplyRouteHistory(context.Background(), applyRouteContextWithObserved(t, route, observed))
	for _, view := range []string{"lagging", "fenced", "other-owner"} {
		t.Run(view, func(t *testing.T) {
			localRoute := route
			localRoute.MinWriteTSExclusive = 900
			version := uint64(7)
			switch view {
			case "lagging":
				version = 5
				localRoute.StagedVisibilityActive = false
				localRoute.MigrationJobID = 0
			case "fenced":
				localRoute.State = distribution.RouteStateWriteFenced
			case "other-owner":
				localRoute.GroupID = 2
			}
			f := &kvFSM{store: store.NewMVCCStore(), routes: applyRouteContextHistory(t, version, localRoute), shardGroupID: 1}
			assertApplyRouteContextHelpers(t, f, ctx)
		})
	}
}

func assertApplyRouteContextHelpers(t *testing.T, f *kvFSM, ctx context.Context) {
	t.Helper()
	key := []byte("k")
	muts := []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("value")}}
	r := &pb.Request{ObservedRouteVersion: 6, Mutations: muts}
	snapshot, err := f.routeFloorSnapshotForRequest(ctx, r)
	require.NoError(t, err)
	require.Equal(t, uint64(6), snapshot.Version())
	require.NoError(t, verifyRouteWriteTimestampFloorForKeyFromSnapshot(snapshot, key, 75))
	require.NoError(t, f.verifyRouteNotFencedForKey(ctx, key))
	require.NoError(t, f.verifyRouteNotFencedForMutations(ctx, muts, nil))
	require.NoError(t, f.verifyRouteNotFencedForPrefix(ctx, key))
	require.NoError(t, f.verifyRouteWriteFloorForKey(ctx, key, 150))
	require.ErrorIs(t, f.verifyRouteWriteFloorForKey(ctx, key, 100), ErrRouteWriteBelowFloor)
	require.NoError(t, f.verifyRouteWriteFloorForPrefix(ctx, key, 150))
	require.ErrorIs(t, f.verifyRouteWriteFloorForPrefix(ctx, key, 100), ErrRouteWriteBelowFloor)
	require.NoError(t, f.verifyRouteWriteFloorForMutations(ctx, muts, 150))
	require.NoError(t, f.verifyWriteFence(ctx, r))
	require.NoError(t, f.verifyComposed1(ctx, r))
	routes, version, proof := f.currentShardRoutesForRouteRange(ctx, key, []byte("l"))
	require.True(t, proof)
	require.Equal(t, uint64(7), version)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(11), routes[0].MigrationJobID)
	deletes := f.stagedVisibilityPrefixDeletesForApply(ctx, key, txnCommonPrefix)
	require.Len(t, deletes, 1)
	require.Equal(t, distribution.MigrationStagedDataKey(11, key), deletes[0].prefix)
	require.Equal(t, distribution.MigrationStagedDataKey(11, txnCommonPrefix), deletes[0].excludePrefix)
}

func TestApplyRouteContextReadinessOCCAndPrefixDelete(t *testing.T) {
	t.Parallel()
	for _, view := range []string{"lagging", "fenced", "other-owner"} {
		t.Run(view, func(t *testing.T) {
			ctx, f := newApplyRouteContextStagedFSM(t, view)
			key := []byte("k")
			stagedKey := distribution.MigrationStagedDataKey(11, key)
			require.NoError(t, f.store.PutAt(ctx, stagedKey, []byte("staged"), 120, 0))
			require.NoError(t, f.store.PutAt(ctx, key, []byte("live"), 110, 0))
			outside := distribution.MigrationStagedDataKey(11, []byte("n"))
			require.NoError(t, f.store.PutAt(ctx, outside, []byte("keep"), 120, 0))
			routes, err := f.targetReadyRoutesForRange(ctx, key, nextScanCursor(key))
			require.NoError(t, err)
			require.Len(t, routes, 1)
			require.Equal(t, uint64(11), routes[0].MigrationJobID)
			latest, exists, err := f.latestCommitTSForTargetReadyKey(ctx, key)
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, uint64(120), latest)
			muts := []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: []byte("new")}}
			require.ErrorIs(t, f.validateConflicts(ctx, muts, 115), store.ErrWriteConflict)
			require.ErrorIs(t, f.validateReadConflicts(ctx, [][]byte{key}, 115), store.ErrWriteConflict)
			readKeys, err := f.onePhaseConflictReadKeys(ctx, muts, [][]byte{key})
			require.NoError(t, err)
			require.Equal(t, [][]byte{key, stagedKey}, readKeys)
			require.NoError(t, f.handleDelPrefix(ctx, key, 150))
			_, err = f.store.GetAt(ctx, key, 150)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			_, err = f.store.GetAt(ctx, stagedKey, 150)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			value, err := f.store.GetAt(ctx, outside, 150)
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), value)
		})
	}
}

func newApplyRouteContextStagedFSM(t *testing.T, view string) (context.Context, *kvFSM) {
	t.Helper()
	route := applyRouteContextDescriptor()
	history := applyRouteContextWithObserved(t, route, route)
	ctx := withApplyRouteHistory(context.Background(), history)
	local := route
	version := uint64(7)
	switch view {
	case "lagging":
		version = 5
		local.MigrationJobID = 0
		local.StagedVisibilityActive = false
	case "fenced":
		local.State = distribution.RouteStateWriteFenced
	case "other-owner":
		local.GroupID = 2
	}
	f := &kvFSM{store: store.NewMVCCStore(), routes: applyRouteContextHistory(t, version, local), shardGroupID: 1}
	writer, ok := f.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
		JobID: 11, RouteStart: []byte("a"), RouteEnd: []byte("z"),
		ExpectedCutoverVersion: 7, MigrationJobID: 11, MinWriteTSExclusive: 100, Armed: true,
	}))
	ownership, ok := f.store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	require.NoError(t, ownership.AcquireMigrationOwnership(ctx, store.MigrationOwnershipState{
		JobID: 11, LocalGroupID: 1, OwnerGroupID: 1, CatalogVersion: 7, ConfigurationIndex: 1,
		RouteStart: []byte("a"), RouteEnd: []byte("z"), MinWriteTSExclusive: 100,
		Role: store.MigrationOwner, Staged: true,
	}, 1))
	return ctx, f
}

func TestApplyRouteContextMissingAndContradictingEvidence(t *testing.T) {
	t.Parallel()
	route := applyRouteContextDescriptor()
	f := &kvFSM{store: store.NewMVCCStore(), routes: applyRouteContextWithObserved(t, route, route), shardGroupID: 1}
	r := &pb.Request{ObservedRouteVersion: 6, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k")}}}
	missing := withApplyRouteHistory(context.Background(), applyRouteContextHistory(t, 7, route))
	_, err := f.routeFloorSnapshotForRequest(missing, r)
	require.ErrorIs(t, err, ErrComposed1VersionGCd)
	require.ErrorIs(t, f.verifyComposed1(missing, r), ErrComposed1VersionGCd)
	require.ErrorIs(t, f.verifyWriteFence(missing, r), ErrComposed1VersionGCd)
	fenced := route
	fenced.State = distribution.RouteStateWriteFenced
	for _, history := range []RouteHistory{
		applyRouteContextWithObserved(t, fenced, route),
		applyRouteContextWithObserved(t, route, fenced),
	} {
		ctx := withApplyRouteHistory(context.Background(), history)
		require.ErrorIs(t, f.verifyWriteFence(ctx, r), ErrRouteWriteFenced)
	}
	ctx := withApplyRouteHistory(context.Background(), applyRouteContextHistory(t, 7, fenced))
	require.ErrorIs(t, f.verifyRouteNotFencedForKey(ctx, []byte("k")), ErrRouteWriteFenced)
	require.ErrorIs(t, f.verifyRouteNotFencedForPrefix(ctx, []byte("k")), ErrRouteWriteFenced)
	other := route
	other.GroupID = 2
	for _, history := range []RouteHistory{
		applyRouteContextWithObserved(t, other, route),
		applyRouteContextWithObserved(t, route, other),
	} {
		require.ErrorIs(t, f.verifyComposed1(withApplyRouteHistory(context.Background(), history), r), ErrComposed1Violation)
	}
}

func TestApplyRouteContextTransactionPaths(t *testing.T) {
	t.Parallel()
	for _, view := range []string{"lagging", "fenced", "other-owner"} {
		t.Run(view, func(t *testing.T) {
			for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
				t.Run(phase.String(), func(t *testing.T) {
					ctx, f := newApplyRouteContextStagedFSM(t, view)
					key := []byte("k")
					require.NoError(t, f.store.PutAt(ctx, distribution.MigrationStagedDataKey(11, key), []byte("old"), 120, 0))
					r := applyRouteContextTxn(phase, 130, 200, key)
					require.NoError(t, f.applyRequestErr(ctx, r))
					if phase == pb.Phase_PREPARE {
						commit := applyRouteContextTxn(pb.Phase_COMMIT, 130, 200, key)
						require.NoError(t, f.applyRequestErr(ctx, commit))
					}
					value, err := f.store.GetAt(ctx, key, 200)
					require.NoError(t, err)
					require.Equal(t, []byte("new"), value)
				})
			}
		})
	}
}

func applyRouteContextTxn(phase pb.Phase, startTS, commitTS uint64, key []byte) *pb.Request {
	return &pb.Request{
		IsTxn: true, Phase: phase, Ts: startTS, ObservedRouteVersion: 7,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
				PrimaryKey: key, LockTTLms: 1000, CommitTS: commitTS,
			})},
			{Op: pb.Op_PUT, Key: key, Value: []byte("new")},
		},
	}
}

func TestApplyRouteContextBatchVerdictsAndLegacyFallback(t *testing.T) {
	t.Parallel()
	for _, pebble := range []bool{false, true} {
		name := "memory"
		if pebble {
			name = "pebble"
		}
		t.Run(name, func(t *testing.T) {
			ctx, f := newApplyRouteContextStagedFSM(t, "fenced")
			if pebble {
				st, err := store.NewPebbleStore(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, st.Close()) })
				f.store = st
			}
			f.pendingApplyIdx = 42
			requests := []*pb.Request{
				{Ts: 100, ObservedRouteVersion: 7, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("too-old")}}},
				{Ts: 150, ObservedRouteVersion: 7, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("valid")}}},
				{Ts: 160, ObservedRouteVersion: 7, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Value: []byte("invalid")}}},
				{Ts: 170, ObservedRouteVersion: 7, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("after-error")}}},
			}
			results := make([]error, len(requests))
			halt, hasError := f.applyBatchedRequests(ctx, requests, results)
			require.Nil(t, halt)
			require.True(t, hasError)
			require.ErrorIs(t, results[0], ErrRouteWriteTimestampTooLow)
			require.NoError(t, results[1])
			require.ErrorIs(t, results[2], ErrInvalidRequest)
			require.NoError(t, results[3])
			value, err := f.store.GetAt(ctx, []byte("k"), 200)
			require.NoError(t, err)
			require.Equal(t, []byte("valid"), value)
			value, err = f.store.GetAt(ctx, []byte("n"), 200)
			require.NoError(t, err)
			require.Equal(t, []byte("after-error"), value)
			require.ErrorIs(t, f.verifyWriteFence(context.Background(), requests[1]), ErrRouteWriteFenced)
			require.ErrorIs(t, f.verifyRouteNotFencedForKey(context.Background(), []byte("k")), ErrRouteWriteFenced)
			if pebble {
				reader, ok := f.store.(interface{ LastAppliedIndex() (uint64, bool, error) })
				require.True(t, ok)
				index, present, err := reader.LastAppliedIndex()
				require.NoError(t, err)
				require.True(t, present)
				require.Equal(t, uint64(42), index)
			}
		})
	}
}
