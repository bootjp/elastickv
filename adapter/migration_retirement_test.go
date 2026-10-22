package adapter

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type retirementVoterProposer struct {
	voters []raftengine.StateMachine
	index  uint64
}

type retirementUnsupportedStore struct{ store.MVCCStore }

type retirementContradictingStore struct{ store.MVCCStore }

func (s retirementContradictingStore) MigrationRetired(context.Context, uint64) (bool, error) {
	return true, nil
}

func (s retirementContradictingStore) MigrationTargetReadinessStates(context.Context) ([]store.TargetStagedReadinessState, error) {
	return []store.TargetStagedReadinessState{{JobID: 7}}, nil
}

func TestMigrationRetirementProbeRequiresWitnessAndAbsentActiveMetadata(t *testing.T) {
	t.Parallel()
	request := &pb.ProbeMigrationStateRequest{JobId: 7, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED}
	server := NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(store.NewMVCCStore()))
	response, err := server.ProbeMigrationState(t.Context(), request)
	require.NoError(t, err)
	require.False(t, response.GetReady(), "absence alone is not retirement")
	server = NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(retirementUnsupportedStore{store.NewMVCCStore()}))
	_, err = server.ProbeMigrationState(t.Context(), request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	server = NewInternalWithEngine(nil, nil, nil, nil, WithInternalStore(retirementContradictingStore{store.NewMVCCStore()}))
	response, err = server.ProbeMigrationState(t.Context(), request)
	require.NoError(t, err)
	require.False(t, response.GetReady(), "retirement cannot hide an active guard")
}

func (p *retirementVoterProposer) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.index++
	var response any
	for _, voter := range p.voters {
		response = voter.Apply(data)
	}
	return &raftengine.ProposalResult{CommitIndex: p.index, Response: response}, nil
}

func (p *retirementVoterProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

type heldRetirementProposer struct {
	*retirementVoterProposer
	command []byte
	started chan struct{}
	release chan struct{}
}

func (p *heldRetirementProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if bytes.Equal(data, p.command) {
		close(p.started)
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.retirementVoterProposer.Propose(ctx, data)
}

func (p *heldRetirementProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

func TestMigrationRetirementRejectsOldRunnerAfterAllVoterCleanupProbes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	engine := distribution.NewEngine()
	route := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7, State: distribution.RouteStateWriteFenced}
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{route}}))
	proposer := &retirementVoterProposer{}
	stores := make([]store.MVCCStore, 3)
	servers := make([]*Internal, 3)
	for index := range stores {
		st := store.NewMVCCStore()
		stores[index] = st
		writer, ok := st.(store.MigrationTargetReadinessWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7,
			RouteStart: []byte("m"), RouteEnd: []byte("z"), Armed: true, TrackWrites: true, MinWriteTSExclusive: 1, RetentionPinTS: 1}))
		proposer.voters = append(proposer.voters, kv.NewKvFSMWithHLC(st, kv.NewHLC(), kv.WithRouteHistory(kv.WrapDistributionEngine(engine), 7)))
		servers[index] = NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st),
			WithInternalMigrationProposer(proposer), WithInternalMigrationCleanupGate(func(context.Context) error { return nil }))
	}
	late := &pb.TargetStagedReadinessRequest{JobId: 7, MigrationJobId: 7, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		Armed: true, SourceWriteFence: true, MinWriteTsExclusive: 500, RetentionPinTs: 1}
	command, err := kv.MarshalTargetStagedReadinessCommand(late)
	require.NoError(t, err)
	held := &heldRetirementProposer{retirementVoterProposer: proposer, command: command, started: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(held.release) })
	t.Cleanup(release)
	oldRunner := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(stores[0]),
		WithInternalMigrationProposer(held), WithInternalMigrationImportGate(func(context.Context) error { return nil }))
	result := make(chan error, 1)
	go func() { _, callErr := oldRunner.ApplyTargetStagedReadiness(ctx, late); result <- callErr }()
	select {
	case <-held.started:
	case callErr := <-result:
		t.Fatalf("old RPC did not reach the proposal barrier: %v", callErr)
	}
	_, err = servers[0].CleanupMigration(ctx, &pb.CleanupMigrationRequest{JobId: 7, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA})
	require.NoError(t, err)
	for _, server := range servers {
		proof, probeErr := server.ProbeMigrationState(ctx, &pb.ProbeMigrationStateRequest{JobId: 7, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_METADATA_CLEARED})
		require.NoError(t, probeErr)
		require.True(t, proof.GetReady())
	}
	route.State = distribution.RouteStateActive
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{Version: 2, Routes: []distribution.RouteDescriptor{route}}))
	release()
	require.ErrorIs(t, <-result, store.ErrMigrationRetired)
	for _, st := range stores {
		require.Equal(t, uint64(1), st.LastCommitTS(), "a late control must not raise the clock floor")
		reader, ok := st.(store.MigrationTargetReadinessReader)
		require.True(t, ok)
		states, readErr := reader.MigrationTargetReadinessStates(ctx)
		require.NoError(t, readErr)
		require.Empty(t, states, "no source guard or retention pin may return")
	}
	txn := kv.NewTransactionWithProposer(proposer)
	t.Cleanup(txn.Close)
	_, err = txn.Commit(ctx, []*pb.Request{{Ts: 100, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("active")}}}})
	require.NoError(t, err)
	for _, st := range stores {
		value, readErr := st.GetAt(ctx, []byte("n"), 100)
		require.NoError(t, readErr)
		require.Equal(t, []byte("active"), value)
	}
}
