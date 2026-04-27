package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
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

func (fakeRaftAdminClient) AddLearner(context.Context, *pb.RaftAdminAddLearnerRequest, ...grpc.CallOption) (*pb.RaftAdminConfigurationChangeResponse, error) {
	return &pb.RaftAdminConfigurationChangeResponse{Index: 2}, nil
}

func (fakeRaftAdminClient) PromoteLearner(context.Context, *pb.RaftAdminPromoteLearnerRequest, ...grpc.CallOption) (*pb.RaftAdminConfigurationChangeResponse, error) {
	return &pb.RaftAdminConfigurationChangeResponse{Index: 3}, nil
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

func TestUsageErrorForCommand(t *testing.T) {
	require.EqualError(t, usageError("add_voter"), "usage: raftadmin <addr> add_voter <id> <address> [previous_index]")
	require.EqualError(t, usageError("add_learner"), "usage: raftadmin <addr> add_learner <id> <address> [previous_index]")
	require.EqualError(t, usageError("promote_learner"), "usage: raftadmin <addr> promote_learner <id> [previous_index] [min_applied_index] [skip_min_applied_check]")
	require.EqualError(t, usageError("remove_server"), "usage: raftadmin <addr> remove_server <id> [previous_index]")
	require.EqualError(t, usageError("leadership_transfer_to_server"), "usage: raftadmin <addr> leadership_transfer_to_server <id> <address>")
}

func TestUsageErrorFallback(t *testing.T) {
	want := "usage: raftadmin <addr> <leader|state|configuration|add_voter|add_learner|promote_learner|remove_server|leadership_transfer|leadership_transfer_to_server> [args]"
	require.EqualError(t, usageError(""), want)
	require.EqualError(t, usageError("unknown"), want)
}

func TestExecuteCommandAddLearnerPrintsIndex(t *testing.T) {
	client := fakeRaftAdminClient{}
	out := captureStdout(t, func() {
		require.NoError(t, executeCommand(context.Background(), client, "add_learner", []string{"node-X", "127.0.0.1:50099", "5"}))
	})
	require.Equal(t, "index: 2\n", out)
}

func TestExecuteCommandPromoteLearnerPrintsIndex(t *testing.T) {
	client := fakeRaftAdminClient{}
	out := captureStdout(t, func() {
		require.NoError(t, executeCommand(context.Background(), client, "promote_learner", []string{"node-X", "5", "100"}))
	})
	require.Equal(t, "index: 3\n", out)
}

func TestAllowInsecurePlaintextLoopbackByDefault(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:50051", "[::1]:50051", "localhost:50051", "passthrough:///bufnet"} {
		allowed, err := allowInsecurePlaintext(addr)
		require.NoError(t, err, addr)
		require.True(t, allowed, addr)
	}
}

func TestAllowInsecurePlaintextRemoteRequiresExplicitOptIn(t *testing.T) {
	allowed, err := allowInsecurePlaintext("10.0.0.12:50051")
	require.NoError(t, err)
	require.False(t, allowed)

	t.Setenv(allowInsecureEnv, "true")
	allowed, err = allowInsecurePlaintext("10.0.0.12:50051")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestTransportCredentialsForRemotePlaintextRejectsByDefault(t *testing.T) {
	_, err := transportCredentialsFor("10.0.0.12:50051")
	require.EqualError(t, err, "plaintext raftadmin to non-loopback address requires RAFTADMIN_ALLOW_INSECURE=true or RAFTADMIN_TLS=true")
}

func TestTransportCredentialsForTLSAndInvalidBoolEnv(t *testing.T) {
	t.Setenv(tlsEnv, "true")
	creds, err := transportCredentialsFor("10.0.0.12:50051")
	require.NoError(t, err)
	require.NotNil(t, creds)

	t.Setenv(tlsEnv, "")
	t.Setenv(allowInsecureEnv, "maybe")
	_, err = allowInsecurePlaintext("10.0.0.12:50051")
	require.EqualError(t, err, `invalid boolean value for RAFTADMIN_ALLOW_INSECURE: "maybe"`)
}

func TestTransportCredentialsForTLSWithCustomCA(t *testing.T) {
	certPath := writeTestCACert(t)
	t.Setenv(tlsEnv, "true")
	t.Setenv(tlsCACertEnv, certPath)

	creds, err := transportCredentialsFor("10.0.0.12:50051")
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestTransportCredentialsForTLSWithMissingCA(t *testing.T) {
	t.Setenv(tlsEnv, "true")
	t.Setenv(tlsCACertEnv, "/does/not/exist.pem")

	_, err := transportCredentialsFor("10.0.0.12:50051")
	require.ErrorContains(t, err, "read RAFTADMIN_TLS_CA_CERT")
}

func writeTestCACert(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "raftadmin test ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	path := t.TempDir() + "/ca.pem"
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(path, pemBytes, 0o600))
	return path
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
