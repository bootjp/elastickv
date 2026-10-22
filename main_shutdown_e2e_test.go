package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const shutdownProcessTestEnv = "ELASTICKV_SHUTDOWN_PROCESS_TEST"

// Run the production startup/shutdown path in a subprocess so SIGTERM cannot
// terminate the test runner or interfere with another test's signal handlers.
func TestShutdownProcess(t *testing.T) {
	if os.Getenv(shutdownProcessTestEnv) != "1" {
		return
	}
	require.NoError(t, flag.CommandLine.Parse(flag.Args()))
	require.NoError(t, ignoreCanceledAfterSignal(run()))
}

func TestShutdownSIGTERMTransfersEveryRaftGroupE2E(t *testing.T) {
	const nodeCount = 3
	groupIDs := []uint64{1, 2}
	baseDir := t.TempDir()
	endpoints, listeners := allocateBootstrapE2EMultiGroupEndpoints(t, nodeCount, groupIDs)
	t.Cleanup(func() { closeBootstrapE2EMultiGroupListeners(listeners) })
	groupPeers := bootstrapGroupPeersArg(endpoints, groupIDs)
	nodes := make([]*bootstrapE2ENode, 0, nodeCount-1)
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	for i := range nodeCount - 1 {
		node, err := startBootstrapE2EMultiGroupNode(baseDir, endpoints[i], listeners[i], true, groupIDs, groupPeers, raftEngineEtcd)
		require.NoError(t, err)
		nodes = append(nodes, node)
	}
	ep := endpoints[nodeCount-1]
	closeBootstrapE2EMultiGroupListeners(listeners[nodeCount-1:])
	process, exited, logPath := startShutdownTestProcess(t, baseDir, ep, groupIDs, groupPeers)

	for _, groupID := range groupIDs {
		moveShutdownTestLeadership(t, nodes, ep, groupID)
	}
	conn, err := grpc.NewClient(ep.raftAddrs[groupIDs[0]], grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	key, value := []byte("shutdown-key"), []byte("before-sigterm")
	require.NoError(t, rawPutWithTimeout(pb.NewRawKVClient(conn), key, value))

	require.NoError(t, process.Signal(syscall.SIGTERM))
	select {
	case err := <-exited:
		require.NoError(t, err, "the production server must exit cleanly on SIGTERM")
	case <-time.After(bootstrapE2EWaitTimeout):
		t.Fatal("the server did not exit after SIGTERM")
	}
	output, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotContains(t, string(output), "shutdown leadership transfer failed")
	require.Equal(t, len(groupIDs), strings.Count(string(output), "shutdown leadership transfer completed"))
	for _, groupID := range groupIDs {
		waitForGroupLeader(t, nodes, groupID, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	}
	connAfter, err := grpc.NewClient(endpoints[0].raftAddrs[groupIDs[0]], grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connAfter.Close() })
	client := pb.NewRawKVClient(connAfter)
	require.Eventually(t, func() bool {
		resp, getErr := rawGetWithTimeout(client, key, bootstrapE2ERPCRequestTimeout)
		return getErr == nil && resp.Exists && bytes.Equal(resp.Value, value)
	}, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	require.Eventually(t, func() bool {
		return rawPutWithTimeout(client, key, []byte("after-sigterm")) == nil
	}, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
}

func startShutdownTestProcess(t *testing.T, baseDir string, ep bootstrapE2EMultiGroupEndpoint, groupIDs []uint64, groupPeers string) (*os.Process, <-chan error, string) {
	t.Helper()
	logPath := filepath.Join(baseDir, "shutdown-process.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = logFile.Close()
		if t.Failed() {
			output, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			t.Log(string(output))
		}
	})
	//nolint:gosec // Re-executes this test binary with local test endpoints and a temporary data directory.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestShutdownProcess$", "--",
		"-raftId="+ep.id, "-raftDataDir="+baseDir,
		"-address="+ep.raftAddrs[groupIDs[0]], "-redisAddress="+ep.redisAddr, "-dynamoAddress="+ep.dynamoAddr,
		"-raftGroups="+raftGroupsArg(ep, groupIDs), "-raftGroupPeers="+groupPeers,
		"-metricsAddress=127.0.0.1:0", "-pprofAddress=")
	cmd.Env = append(os.Environ(), shutdownProcessTestEnv+"=1")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
	})
	return cmd.Process, exited, logPath
}

func moveShutdownTestLeadership(t *testing.T, nodes []*bootstrapE2ENode, ep bootstrapE2EMultiGroupEndpoint, groupID uint64) {
	t.Helper()
	conn, err := grpc.NewClient(ep.raftAddrs[groupID], grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := pb.NewRaftAdminClient(conn)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), bootstrapE2ERPCRequestTimeout)
		defer cancel()
		status, err := client.Status(ctx, &pb.RaftAdminStatusRequest{})
		if err != nil {
			return false
		}
		if status.State == pb.RaftAdminState_RAFT_ADMIN_STATE_LEADER {
			return true
		}
		for _, node := range nodes {
			engine := node.engineForGroup(groupID)
			if engine.State() == raftengine.StateLeader {
				admin, ok := engine.(raftengine.Admin)
				return ok && admin.TransferLeadershipToServer(ctx, ep.id, ep.raftAddrs[groupID]) == nil
			}
		}
		return false
	}, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
}
