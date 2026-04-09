package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeRaftAdminClient struct {
	statusResp *pb.RaftAdminStatusResponse
}

func (f fakeRaftAdminClient) Status(context.Context, *pb.RaftAdminStatusRequest, ...grpc.CallOption) (*pb.RaftAdminStatusResponse, error) {
	return f.statusResp, nil
}

func (fakeRaftAdminClient) Configuration(context.Context, *pb.RaftAdminConfigurationRequest, ...grpc.CallOption) (*pb.RaftAdminConfigurationResponse, error) {
	return &pb.RaftAdminConfigurationResponse{}, nil
}

func (fakeRaftAdminClient) AddVoter(context.Context, *pb.RaftAdminAddVoterRequest, ...grpc.CallOption) (*pb.RaftAdminConfigurationChangeResponse, error) {
	return &pb.RaftAdminConfigurationChangeResponse{Index: 1}, nil
}

func (fakeRaftAdminClient) RemoveServer(context.Context, *pb.RaftAdminRemoveServerRequest, ...grpc.CallOption) (*pb.RaftAdminConfigurationChangeResponse, error) {
	return &pb.RaftAdminConfigurationChangeResponse{Index: 1}, nil
}

func (fakeRaftAdminClient) TransferLeadership(context.Context, *pb.RaftAdminTransferLeadershipRequest, ...grpc.CallOption) (*pb.RaftAdminTransferLeadershipResponse, error) {
	return &pb.RaftAdminTransferLeadershipResponse{}, nil
}

func TestExecuteCommandLeaderAndStateOutput(t *testing.T) {
	client := fakeRaftAdminClient{
		statusResp: &pb.RaftAdminStatusResponse{
			State:         pb.RaftAdminState_RAFT_ADMIN_STATE_LEADER,
			LeaderId:      "n1",
			LeaderAddress: "127.0.0.1:50051",
		},
	}

	out := captureStdout(t, func() {
		require.NoError(t, executeCommand(context.Background(), client, "leader", nil))
	})
	require.Contains(t, out, "id: \"n1\"")
	require.Contains(t, out, "address: \"127.0.0.1:50051\"")

	out = captureStdout(t, func() {
		require.NoError(t, executeCommand(context.Background(), client, "state", nil))
	})
	require.Equal(t, "state: LEADER\n", out)
}

func TestRPCTimeoutHonorsEnvSeconds(t *testing.T) {
	t.Setenv(timeoutEnv, "17")
	timeout, err := rpcTimeout()
	require.NoError(t, err)
	require.Equal(t, 17*time.Second, timeout)
}

func TestRPCTimeoutRejectsInvalidEnv(t *testing.T) {
	t.Setenv(timeoutEnv, "abc")
	_, err := rpcTimeout()
	require.Error(t, err)
}

func captureStdout(t *testing.T, run func()) string {
	t.Helper()

	original := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = original })

	run()

	require.NoError(t, writer.Close())
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	os.Stdout = original
	return buf.String()
}
