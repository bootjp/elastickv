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
