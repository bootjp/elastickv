package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type migrationConfigurationLeader struct {
	mockInternalLeader
	index uint64
}

func (l migrationConfigurationLeader) AppliedConfigurationIndex() (uint64, bool) {
	return l.index, l.index != 0
}

func TestMigrationConfigurationProbeRequiresOrderedRuntimeHook(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, nil)
	aware, ok := fsm.(raftengine.AppliedConfigurationAware)
	require.True(t, ok)
	require.NoError(t, aware.RecordAppliedConfiguration(9))
	req := &pb.ProbeMigrationStateRequest{JobId: 7,
		Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_SOURCE_READ_DRAINED, RequireConfigurationIndex: true}
	unsupported := NewInternalWithEngine(nil, mockInternalLeader{}, nil, nil, WithInternalStore(st))
	resp, err := unsupported.ProbeMigrationState(ctx, req)
	require.NoError(t, err)
	require.False(t, resp.GetReady(), "a copied durable row cannot make an unsupported runtime attest")
	require.Zero(t, resp.GetAppliedConfigurationIndex())
	supported := NewInternalWithEngine(nil, migrationConfigurationLeader{index: 9}, nil, nil, WithInternalStore(st))
	resp, err = supported.ProbeMigrationState(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.GetReady())
	require.Equal(t, uint64(9), resp.GetAppliedConfigurationIndex())
	req.ExpectedConfigurationIndex = 8
	resp, err = supported.ProbeMigrationState(ctx, req)
	require.NoError(t, err)
	require.False(t, resp.GetReady())
}

func TestSplitCleanupConfigurationBarrierRejectsMixedAndMissingEpochs(t *testing.T) {
	t.Parallel()
	for _, secondIndex := range []uint64{0, 9, 10} {
		t.Run(map[uint64]string{0: "old runtime", 9: "same configuration", 10: "new configuration"}[secondIndex], func(t *testing.T) {
			n1 := &splitMigrationClientStub{probeFn: func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: 9}, nil
			}}
			n2 := &splitMigrationClientStub{probeFn: func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				return &pb.ProbeMigrationStateResponse{Ready: true, AppliedConfigurationIndex: secondIndex}, nil
			}}
			server := &DistributionServer{splitMigrationVoterFactory: func(context.Context, uint64) ([]SplitMigrationVoter, error) {
				return []SplitMigrationVoter{{ID: "n1", Address: "n1", Client: n1}, {ID: "n2", Address: "n2", Client: n2}}, nil
			}}
			cursor, complete, err := server.syncSplitMigrationVoterBarrier(t.Context(), 7, nil, nil,
				&pb.ProbeMigrationStateRequest{JobId: 7, RequireConfigurationIndex: true})
			require.NoError(t, err)
			require.Equal(t, secondIndex == 9, complete)
			_, err = splitCleanupCursorConfiguration(cursor)
			if complete {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errSplitMigrationVoterBarrierIncomplete)
			}
		})
	}
}

type controlConfigurationReadHook struct {
	store.MVCCStore
	after func()
}

func (s *controlConfigurationReadHook) MigrationTargetReadinessStates(ctx context.Context) ([]store.TargetStagedReadinessState, error) {
	reader, ok := s.MVCCStore.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil, store.ErrNotSupported
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if s.after != nil {
		s.after()
	}
	return states, err
}

func TestMigrationControlProbeRejectsConfigurationChangedDuringRead(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	writer, ok := st.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	state := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true, RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 3, RetentionPinTS: 1, MinWriteTSExclusive: 10}
	require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), state))
	node := NewInternalWithEngine(nil, migrationConfigurationLeader{index: 8}, nil, nil, WithInternalStore(st))
	reader := &controlConfigurationReadHook{MVCCStore: st, after: func() { node.leader = migrationConfigurationLeader{index: 9} }}
	node.store = reader
	req := &pb.ProbeMigrationStateRequest{Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED, JobId: 7, MigrationJobId: 7, TrackWrites: true, RouteStart: state.RouteStart, RouteEnd: state.RouteEnd, ExpectedCatalogVersion: 3, RetentionPinTs: 1, MinWriteTsExclusive: 10, RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9}
	resp, err := node.ProbeMigrationState(t.Context(), req)
	require.NoError(t, err)
	require.False(t, resp.Ready, "an old configuration's read must not be relabeled as the new configuration")
	reader.after = nil
	resp, err = node.ProbeMigrationState(t.Context(), req)
	require.NoError(t, err)
	require.True(t, resp.Ready, "a fresh read in the expected configuration may ACK")
}
