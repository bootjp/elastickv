package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInternalForwardAdminProposalRejectsOtherRaftPayloads(t *testing.T) {
	t.Parallel()
	for opcode := 0; opcode <= 255; opcode++ {
		payload := []byte{byte(opcode), 0}
		proposer := &internalAdminProposer{}
		server := NewInternalWithEngine(nil,
			internalAdminLeaderView(raftengine.StateLeader, 0), nil, nil,
			WithInternalAdminProposer(proposer))
		_, err := server.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{Payload: payload})
		require.Equal(t, codes.PermissionDenied, status.Code(err), "opcode %x", opcode)
		require.Nil(t, proposer.payload)
	}
}

func TestInternalForwardAdminProposalValidatesEntireEnvelope(t *testing.T) {
	t.Parallel()
	valid := kv.EncodeBackupReleaseEntry(kv.BackupReleaseEntry{})
	for _, payload := range [][]byte{nil, valid[:len(valid)-1], append(valid, 0x0d), {0x0e, 0xff}} {
		proposer := &internalAdminProposer{}
		server := NewInternalWithEngine(nil,
			internalAdminLeaderView(raftengine.StateLeader, 0), nil, nil,
			WithInternalAdminProposer(proposer))
		_, err := server.ForwardAdminProposal(context.Background(), &pb.ForwardAdminProposalRequest{Payload: payload})
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Nil(t, proposer.payload)
	}
}
