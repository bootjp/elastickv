package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newCASCatalogFSM(t *testing.T, st store.MVCCStore, watcher *distribution.Engine) (*kvFSM, raftbootstrap.PolicyRecord) {
	t.Helper()
	policy := guardedNewPolicy()
	policy.Identity.Group = 1
	seedGuardedNewPolicy(t, st, policy, 10)
	fsm, ok := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(WrapDistributionEngine(watcher), policy.Identity.Group),
		WithRaftBootstrapIdentity(policy.Identity.Cluster, policy.Identity.Group)).(*kvFSM)
	require.True(t, ok)
	plan, err := distribution.NewCatalogStore(st).BuildBootstrapPlanAt(t.Context(), 1, []distribution.Route{
		{GroupID: policy.Identity.Group, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	applyCASCatalogCommit(t, fsm, policy, 11, 1, 2, plan.Mutations)
	return fsm, policy
}

func applyCASCatalogCommit(t *testing.T, f *kvFSM, policy raftbootstrap.PolicyRecord, index, startTS, commitTS uint64, muts []*store.KVPairMutation) {
	t.Helper()
	key := distribution.CatalogVersionKey()
	req := newOnePhaseTxnRequest(startTS, commitTS, 0, key, len(muts), nil, 0, nil)
	for _, mut := range muts {
		req.Mutations = append(req.Mutations, &pb.Mutation{Op: pb.Op_PUT, Key: mut.Key, Value: mut.Value})
		req.ReadKeys = append(req.ReadKeys, mut.Key)
	}
	command := guardedNewCommand(t, policy, raftbootstrap.ControlGuardedCatalog)
	command.OperationID = raftbootstrap.OperationID{byte(index)}
	command.Inner = encodeGuardedCatalogRequest(t, req)
	result := applyGuardedNewCommand(t, f, command, index)
	receipt, ok := result.(raftbootstrap.GuardedReceipt)
	require.True(t, ok, "catalog apply returned %v", result)
	require.Equal(t, raftbootstrap.GuardedReceiptApplied, receipt.Verdict)
}

func applyCASCatalogRoutes(t *testing.T, f *kvFSM, policy raftbootstrap.PolicyRecord, index, commitTS uint64, snapshot distribution.CatalogSnapshot) {
	t.Helper()
	muts := []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: distribution.CatalogVersionKey(), Value: distribution.EncodeCatalogVersion(snapshot.Version)},
		{Op: store.OpTypePut, Key: distribution.CatalogNextRouteIDKey(), Value: distribution.EncodeCatalogNextRouteID(3)},
	}
	delta := distribution.CatalogDelta{Version: snapshot.Version, PreviousVersion: snapshot.Version - 1}
	for _, route := range snapshot.Routes {
		raw, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
		require.NoError(t, err)
		muts = append(muts, &store.KVPairMutation{Op: store.OpTypePut, Key: distribution.CatalogRouteKey(route.RouteID), Value: raw})
		delta.Mutations = append(delta.Mutations, distribution.CatalogRouteMutation{
			Op: distribution.CatalogMutationUpsert, RouteID: route.RouteID, Route: route,
		})
	}
	startTS := f.store.LastCommitTS()
	deltaMuts, err := distribution.NewCatalogStore(f.store).BuildDeltaMutationsAt(t.Context(), startTS, delta)
	require.NoError(t, err)
	applyCASCatalogCommit(t, f, policy, index, startTS, commitTS, append(muts, deltaMuts...))
}

type casCatalogProposer func(context.Context, []byte) (*raftengine.ProposalResult, error)

func (p casCatalogProposer) Propose(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	return p(ctx, payload)
}

func (p casCatalogProposer) ProposeAdmin(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	return p(ctx, payload)
}

func TestVersionCASOrdersCatalogChangeBetweenAdmissionAndApply(t *testing.T) {
	for _, tc := range []struct {
		name  string
		route distribution.RouteDescriptor
		want  error
	}{
		{name: "split", route: distribution.RouteDescriptor{GroupID: 1, State: distribution.RouteStateActive}},
		{name: "move", route: distribution.RouteDescriptor{GroupID: 2, State: distribution.RouteStateActive}, want: ErrRouteWriteFenced},
		{name: "fence", route: distribution.RouteDescriptor{GroupID: 1, State: distribution.RouteStateWriteFenced}, want: ErrRouteWriteFenced},
		{name: "staged visibility", route: distribution.RouteDescriptor{GroupID: 1, State: distribution.RouteStateActive,
			StagedVisibilityActive: true, MigrationJobID: 7, MinWriteTSExclusive: 12}, want: ErrVersionCASUnavailable},
		{name: "write floor", route: distribution.RouteDescriptor{GroupID: 1, State: distribution.RouteStateActive,
			MinWriteTSExclusive: 12}, want: ErrVersionCASUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			watcher := distribution.NewEngine()
			require.NoError(t, watcher.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{
				{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive},
			}}))
			f, policy := newCASCatalogFSM(t, st, watcher)
			activateCommitTSFence(t, f)
			f.SetApplyIndex(12)
			require.Nil(t, f.Apply(EncodeMigrationVersionIdentityActivation()))
			require.NoError(t, st.PutAt(t.Context(), []byte("n"), []byte("before"), 10, 0))

			proposals := 0
			right := tc.route
			right.RouteID, right.Start = 2, []byte("m")
			proposer := casCatalogProposer(func(_ context.Context, payload []byte) (*raftengine.ProposalResult, error) {
				proposals++
				req, err := DecodeVersionCASRaftPayload(payload)
				require.NoError(t, err)
				require.EqualValues(t, 1, req.GetObservedRouteVersion())
				require.EqualValues(t, 1, req.GetVersionCasGroupId())
				applyCASCatalogRoutes(t, f, policy, 13, 12, distribution.CatalogSnapshot{Version: 2, Routes: []distribution.RouteDescriptor{
					{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive, End: []byte("m")},
					right,
				}})
				f.SetApplyIndex(14)
				return &raftengine.ProposalResult{CommitIndex: 14, Response: f.Apply(payload)}, nil
			})
			tx := NewTransactionWithProposer(proposer, WithMigrationRequestAdmission(st, 1),
				WithVersionCASAdmission(func(ctx context.Context) (context.Context, error) {
					return raftengine.WithVersionCASProof(ctx, raftengine.VersionCASProof{Term: 3, ConfigurationIndex: 5}), nil
				}))
			t.Cleanup(tx.Close)
			req := casApplyRequest(15, 20, 10, "n", "after")
			req.ObservedRouteVersion = 0
			_, err = tx.Commit(t.Context(), []*pb.Request{req})
			wantTS := uint64(20)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
				var rejection *AppliedRequestRejection
				require.ErrorAs(t, err, &rejection)
				require.EqualValues(t, 14, rejection.CommitIndex())
				wantTS = 10
				_, err = tx.Commit(t.Context(), []*pb.Request{req})
				require.ErrorIs(t, err, ErrVersionCASUnavailable, "stale retries must stop before proposal")
			}
			require.Equal(t, 1, proposals)
			require.EqualValues(t, 1, watcher.Version(), "watcher stayed behind throughout")
			requireCASProgress(t, f, 14)
			row, err := store.GetVersionAt(t.Context(), st, []byte("n"), 30)
			require.NoError(t, err)
			require.Equal(t, wantTS, row.TS)
		})
	}
}

func TestVersionCASRequiresCatalogDescriptorRangeInGroup(t *testing.T) {
	f, st := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, f)
	_, err := admitVersionCASRoute(t.Context(), st, 1, []byte("key"))
	require.NoError(t, err)
	catalog := distribution.NewCatalogStore(st)
	_, err = catalog.Save(t.Context(), 1, []distribution.RouteDescriptor{
		{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive, End: []byte("!dist|route|")},
		{RouteID: 2, GroupID: 2, State: distribution.RouteStateActive, Start: []byte("!dist|route|"), End: []byte("a")},
		{RouteID: 3, GroupID: 1, State: distribution.RouteStateActive, Start: []byte("a")},
	})
	require.NoError(t, err)
	_, err = admitVersionCASRoute(t.Context(), st, 1, []byte("key"))
	require.ErrorIs(t, err, ErrVersionCASUnavailable)
}

func TestVersionCASPreservesBackupTimestampFloor(t *testing.T) {
	f, st := newCommitTSFenceFSM(t)
	f.readTracker = NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	enableCASApplyForTest(t, f)
	require.NoError(t, st.PutAt(t.Context(), []byte("key"), []byte("before"), 10, 0))
	f.SetApplyIndex(2)
	require.Nil(t, f.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: 25, Deadline: time.Now().Add(time.Hour),
	})))
	requireCASRejection(t, applyCASFrame(t, f, 3, casApplyRequest(15, 20, 10, "key", "after")), ErrBackupTimestampFenced)
	requireCASProgress(t, f, 3)
	row, err := store.GetVersionAt(t.Context(), st, []byte("key"), 30)
	require.NoError(t, err)
	require.EqualValues(t, 10, row.TS)
}

func TestVersionCASFrameRejectsWrongGroup(t *testing.T) {
	f, st := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, f)
	require.NoError(t, st.PutAt(t.Context(), []byte("key"), []byte("before"), 10, 0))
	req := casApplyRequest(15, 20, 10, "key", "after")
	req.VersionCasGroupId = 2
	requireCASRejection(t, applyCASFrame(t, f, 2, req), ErrRouteWriteFenced)
	requireCASProgress(t, f, 2)
	row, err := store.GetVersionAt(t.Context(), st, []byte("key"), 30)
	require.NoError(t, err)
	require.EqualValues(t, 10, row.TS)
}

func TestVersionCASFrameHaltsWithoutAppliedCatalog(t *testing.T) {
	f, st := newCommitTSFenceFSM(t)
	bindCASTestGroup(f)
	enableCASMarkersForTest(t, f)
	require.NoError(t, st.PutAt(t.Context(), []byte("key"), []byte("before"), 10, 0))
	resp := applyCASFrame(t, f, 2, casApplyRequest(15, 20, 10, "key", "after"))
	require.ErrorContains(t, haltApplyOf(resp), "lacks an applied catalog")
	requireCASProgress(t, f, 1)
	row, err := store.GetVersionAt(t.Context(), st, []byte("key"), 30)
	require.NoError(t, err)
	require.EqualValues(t, 10, row.TS)
}

func TestVersionCASLegacyFrameHaltsWithoutAdvancing(t *testing.T) {
	f, _ := newCommitTSFenceFSM(t)
	enableCASApplyForTest(t, f)
	req := casApplyRequest(15, 20, 10, "key", "after")
	req.VersionCasGroupId = 0
	body, err := proto.Marshal(req)
	require.NoError(t, err)
	f.SetApplyIndex(2)
	result := f.Apply(append([]byte("\x06CAS\x01"), body...))
	require.ErrorContains(t, haltApplyOf(result), "unsupported version CAS Raft frame")
	requireCASProgress(t, f, 1)
}
