package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestMigrationValueAdmissionIsScopedAndPreMutation(t *testing.T) {
	ctx := context.Background()
	large := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	fsm := &kvFSM{store: store.NewMVCCStore()}
	writer, ok := fsm.store.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	state := store.TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, RouteStart: []byte("m"), RouteEnd: []byte("z"),
		Armed: true, TrackWrites: true, RetentionPinTS: 1, MinWriteTSExclusive: 1}
	require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, state))
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
		for _, transactional := range []bool{false, true} {
			req := &pb.Request{IsTxn: transactional, Phase: phase, Ts: 10,
				Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: large}}}
			err := fsm.applyRequestErr(ctx, req)
			require.ErrorIs(t, err, ErrMigrationValueTooLarge)
			require.NoError(t, haltApplyOf(applyErrorResponse(err)))
			require.Equal(t, uint64(1), fsm.store.LastCommitTS())
		}
	}
	reader, ok := fsm.store.(store.MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := reader.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Equal(t, []store.TargetStagedReadinessState{state}, states, "rejection cannot advance the tracker")
	require.NoError(t, fsm.applyRequestErr(ctx, &pb.Request{Ts: 10,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("small")}}}))
	require.NoError(t, fsm.verifyMigrationValueAdmission(ctx, &pb.Request{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("a"), Value: large}}}), "unrelated ranges retain their old limit")
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		require.NoError(t, fsm.verifyMigrationValueAdmission(ctx, &pb.Request{IsTxn: true, Phase: phase,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: large}}}), "preexisting prepares must drain")
	}
	cleaner, ok := fsm.store.(store.MigrationCleaner)
	require.True(t, ok)
	require.NoError(t, cleaner.ClearMigrationState(ctx, 7, 0))
	require.NoError(t, fsm.applyRequestErr(ctx, &pb.Request{Ts: 20,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: large}}}), "normal large writes resume after retirement")
}
