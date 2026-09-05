package adapter

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestSplitMigrationVoterBarrierReopensForMembershipChanges(t *testing.T) {
	t.Parallel()

	ready := func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		return &pb.ProbeMigrationStateResponse{Ready: true}, nil
	}
	blocked := func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
		return &pb.ProbeMigrationStateResponse{}, nil
	}
	n1 := &splitMigrationClientStub{probeFn: ready}
	n2 := &splitMigrationClientStub{probeFn: ready}
	n3 := &splitMigrationClientStub{probeFn: blocked}
	voters := []SplitMigrationVoter{
		{ID: "n1", Address: "n1:50051", Client: n1},
		{ID: "n2", Address: "n2:50051", Client: n2},
	}
	server := &DistributionServer{splitMigrationVoterFactory: func(context.Context, uint64) ([]SplitMigrationVoter, error) {
		return voters, nil
	}}
	req := &pb.ProbeMigrationStateRequest{JobId: 7, Kind: pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_CONTROL_APPLIED}

	cursor, complete, err := server.syncSplitMigrationVoterBarrier(context.Background(), 1, nil, nil, req)
	require.NoError(t, err)
	require.True(t, complete)
	acks, err := decodeSplitVoterAckCursor(cursor)
	require.NoError(t, err)
	require.Len(t, acks, 2)

	voters = append(voters, SplitMigrationVoter{ID: "n3", Address: "n3:50051", Client: n3})
	cursor, complete, err = server.syncSplitMigrationVoterBarrier(context.Background(), 1, cursor, nil, req)
	require.NoError(t, err)
	require.False(t, complete)
	acks, err = decodeSplitVoterAckCursor(cursor)
	require.NoError(t, err)
	require.Len(t, acks, 3)
	require.False(t, acks[2].acked)

	n3.probeFn = ready
	_, complete, err = server.syncSplitMigrationVoterBarrier(context.Background(), 1, cursor, nil, req)
	require.NoError(t, err)
	require.True(t, complete)

	n2.probeFn = blocked
	voters[1].Address = "n2:50052"
	_, complete, err = server.syncSplitMigrationVoterBarrier(context.Background(), 1, cursor, nil, req)
	require.NoError(t, err)
	require.False(t, complete, "an address change must be treated as a new voter endpoint")
}

func TestSplitVoterAckCursorRejectsCorruption(t *testing.T) {
	t.Parallel()

	_, err := decodeSplitVoterAckCursor([]byte{99})
	require.Error(t, err)
	_, err = decodeSplitVoterAckCursor([]byte{splitVoterAckCursorVersion, 1, 4, 'n'})
	require.Error(t, err)
}
