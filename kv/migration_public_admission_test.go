package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type migrationAdmissionProposer struct {
	fsm       *kvFSM
	proposals uint64
}

func (p *migrationAdmissionProposer) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.proposals++
	index := uint64(20) + p.proposals
	p.fsm.SetApplyIndex(index)
	return &raftengine.ProposalResult{CommitIndex: index, Response: p.fsm.Apply(data)}, nil
}

func (p *migrationAdmissionProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

func TestMigrationProposalQueueRevalidatesIndependentRequests(t *testing.T) {
	f := newComposed1FSM(t, staleServingEngine(t, 1), 1)
	p := &migrationAdmissionProposer{fsm: f}
	tx := NewTransactionWithProposer(p, WithMigrationRequestAdmission(f.store, 1))
	t.Cleanup(tx.Close)
	require.True(t, MigrationRequestAdmissionReady(tx))
	// These are independent queued requests. A whole client request crossing
	// both ranges is rejected atomically; unrelated queue items still apply.
	rejected := &rawCommitItem{reqs: []*pb.Request{{Ts: 50, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte("b"), Value: []byte("must-not-leak")},
		{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("unproven")},
	}}}, done: make(chan rawCommitResult, 1)}
	accepted := &rawCommitItem{reqs: []*pb.Request{{Ts: 51, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("ordinary")}}}}, done: make(chan rawCommitResult, 1)}
	// The guard arrives after queuing, before the common proposal hook runs.
	deterministicMigrationGuard(t, f.store, true)
	tx.applyRawBatch([]*rawCommitItem{rejected, accepted})
	require.ErrorIs(t, (<-rejected.done).err, ErrRouteCutoverPending)
	require.NoError(t, (<-accepted.done).err)
	require.Equal(t, uint64(1), p.proposals)
	_, err := f.store.GetAt(t.Context(), []byte("b"), 100)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	value, err := f.store.GetAt(t.Context(), []byte("c"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("ordinary"), value)
}

func TestMigrationProposalAdvisoryPreservesAuthenticatedResolution(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			f := &kvFSM{store: store.NewMVCCStore(), shardGroupID: 1}
			request := applyRouteContextTxn(pb.Phase_PREPARE, 20, 60, []byte("n"))
			require.NoError(t, f.applyRequestErr(t.Context(), request))
			retireServingSource(t, f.store)
			f.routes = forbiddenApplyWatcher{}
			tx := NewTransactionWithProposer(&migrationAdmissionProposer{fsm: f}, WithMigrationRequestAdmission(f.store, 1))
			t.Cleanup(tx.Close)
			request.Phase = phase
			_, err := tx.Commit(t.Context(), []*pb.Request{request})
			require.NoError(t, err)
			_, err = tx.Commit(t.Context(), []*pb.Request{request})
			require.NoError(t, err, "durable resolution retry must remain a no-op")
		})
	}
}

func TestMigrationPublicReadsHonorNodeFloor(t *testing.T) {
	st := store.NewMVCCStore()
	retireServingSource(t, st)
	shards := NewShardStore(staleServingEngine(t, 9), map[uint64]*ShardGroup{1: {Store: st}})
	require.True(t, shards.MigrationReadAdmissionReady(1))
	checks := []func() error{
		func() error { _, e := shards.ScanAt(t.Context(), []byte("b"), []byte("c"), 1, 200); return e },
		func() error { _, e := shards.ReverseScanAt(t.Context(), []byte("b"), []byte("c"), 1, 200); return e },
		func() error { _, e := shards.ScanGroupAt(t.Context(), 1, []byte("b"), []byte("c"), 1, 200); return e },
		func() error {
			_, e := shards.ReverseScanGroupAt(t.Context(), 1, []byte("b"), []byte("c"), 1, 200)
			return e
		},
		func() error {
			_, e := shards.ScanGroupKeysAt(t.Context(), 1, []byte("b"), []byte("c"), 1, 200)
			return e
		},
		func() error {
			_, _, e := shards.ScanAtPhysicalLimit(t.Context(), []byte("b"), []byte("c"), 1, 1, 200)
			return e
		},
		func() error {
			_, _, e := shards.ReverseScanAtPhysicalLimit(t.Context(), []byte("b"), []byte("c"), 1, 1, 200)
			return e
		},
		func() error { _, e := shards.CommittedVersionAt(t.Context(), []byte("b"), 200); return e },
	}
	for i, check := range checks {
		require.ErrorIs(t, check(), ErrRouteCutoverPending, "entry point %d", i)
	}
	coordinator := &ShardedCoordinator{engine: shards.engine, groups: shards.groups}
	require.ErrorIs(t, coordinator.verifyTargetReadinessForReadKeyOnShard(t.Context(), 1, shards.groups[1], []byte("b")), ErrRouteCutoverPending)
}

func TestLegacyUnknownOwnershipBlocksOnlyItsRange(t *testing.T) {
	st := store.NewMVCCStore()
	writer, ok := st.(store.MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), store.TargetStagedReadinessState{
		JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true, SourceWriteFence: true,
		RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 10, MinWriteTSExclusive: 100, RetentionPinTS: 1,
	}, 8))
	proofs, ok := st.(store.MigrationOwnershipStore)
	require.True(t, ok)
	require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), store.MigrationOwnershipState{
		JobID: 7, LocalGroupID: 1, OwnerGroupID: 1, CatalogVersion: 10, ConfigurationIndex: 3,
		MinWriteTSExclusive: 100, Role: store.MigrationOwner, RouteStart: []byte("m"), RouteEnd: []byte("z"),
	}, 9))
	require.NoError(t, st.PutAt(t.Context(), []byte("b"), []byte("ordinary"), 150, 0))
	engine := staleServingEngine(t, 9)
	shards := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: st}})
	value, err := shards.GetAt(t.Context(), []byte("b"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("ordinary"), value)
	_, err = shards.GetAt(t.Context(), []byte("n"), 200)
	require.ErrorIs(t, err, ErrRouteCutoverPending)
	f, ok := NewKvFSMWithHLC(st, nil, WithRouteHistory(WrapDistributionEngine(engine), 1)).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, f.applyRequestErr(t.Context(), &pb.Request{Ts: 200, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("c"), Value: []byte("ordinary")}}}))
	require.ErrorIs(t, f.applyRequestErr(t.Context(), deterministicRawRequest(10, "n")), ErrRouteCutoverPending)
}
