package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	offloadAcceptanceNodeCount = 3
	offloadAcceptanceBatchSize = 12
	offloadAcceptanceTimeout   = 20 * time.Second
	offloadAcceptancePoll      = 25 * time.Millisecond
)

// Reuses the real Raft/Pebble/gRPC bootstrap fixture: no prebuilt snapshot,
// mocked leadership, in-memory object backend or manual publication call.
func TestSnapshotOffloadThreeNodeRecoveryAcceptance(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "4")
	configureOffloadAcceptance(t)
	baseDir, endpoints, nodes := startBootstrapE2ECluster(t, t.TempDir(), offloadAcceptanceNodeCount, 1, raftEngineEtcd)
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	waitForBootstrapClusterConfig(t, nodes, bootstrapExpectedConfiguration(endpoints))
	leader := waitForSingleLeader(t, nodes)
	groupID := nodes[leader].runtimes[0].spec.id
	objects, err := snapshotoffload.NewLocalStore(*snapshotOffloadLocalDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, objects.Close()) })
	clients, conns := rawKVClients(t, endpoints)
	t.Cleanup(func() { closeGRPCConns(conns) })

	workers := make([]func(), len(nodes))
	for i, node := range nodes {
		workers[i] = startOffloadAcceptanceWorker(t, baseDir, node)
	}
	expected := make(map[string]string)
	writeOffloadAcceptanceBatch(t, clients[leader], expected, 0)
	first := checkpointOffloadAcceptance(t, objects, nodes[leader], clients[leader], groupID)
	require.Len(t, first.ConfState.Voters, offloadAcceptanceNodeCount)

	// Lose one replica without changing membership. The surviving quorum must
	// still serve writes while publication and reclamation are running.
	follower := (leader + 1) % len(nodes)
	workers[follower]()
	require.NoError(t, nodes[follower].close())
	nodes[follower] = nil
	for i, node := range nodes {
		if node == nil {
			continue
		}
		workers[i]()
		*snapshotOffloadGCEnabled = i == leader
		workers[i] = startOffloadAcceptanceWorker(t, baseDir, node)
	}
	writeOffloadAcceptanceBatch(t, clients[leader], expected, offloadAcceptanceBatchSize)
	second := checkpointOffloadAcceptance(t, objects, nodes[leader], clients[leader], groupID)
	require.Greater(t, second.SnapshotIndex, first.SnapshotIndex)
	require.NotEqual(t, first.Payload.Key, second.Payload.Key)
	require.Eventually(t, func() bool {
		_, manifestExists, manifestErr := objects.HeadObject(context.Background(), first.ManifestKey)
		_, payloadExists, payloadErr := objects.HeadObject(context.Background(), first.Payload.Key)
		return manifestErr == nil && payloadErr == nil && !manifestExists && !payloadExists
	}, offloadAcceptanceTimeout, offloadAcceptancePoll, "runtime GC must actually reclaim the old manifest and payload")
	writeOffloadAcceptanceBatch(t, clients[leader], expected, 2*offloadAcceptanceBatchSize)
	latest := checkpointOffloadAcceptance(t, objects, nodes[leader], clients[leader], groupID)
	require.Greater(t, latest.SnapshotIndex, second.SnapshotIndex)
	for i, node := range nodes {
		if node != nil {
			requireOffloadAcceptanceLocalValues(t, node, expected)
			requireOffloadAcceptanceValues(t, clients[i], expected)
		}
	}
	for _, stop := range workers {
		stop()
	}
	closeBootstrapE2ENodes(t, nodes)
	clear(nodes)
	// Re-open the object root after all publishers/GC stop. Recovery must use
	// durable bytes, not a live publisher's memory or the source's data dir.
	require.NoError(t, objects.Close())
	objects, err = snapshotoffload.NewLocalStore(*snapshotOffloadLocalDir)
	require.NoError(t, err)
	latest = waitForOffloadAcceptanceManifest(t, objects, groupID, latest.SnapshotIndex)
	t.Logf("source: three voters, one stopped; %d verified user keys plus checkpoint writes; periodic indexes %d -> %d -> %d; old manifest/payload reclaimed",
		len(expected), first.SnapshotIndex, second.SnapshotIndex, latest.SnapshotIndex)
	restoreOffloadAcceptanceCluster(t, objects, latest, expected)
}

func configureOffloadAcceptance(t *testing.T) {
	t.Helper()
	configureSnapshotGC(t)
	withOffloadFlags(t, "", t.TempDir())
	interval, jitter, timeout := *snapshotOffloadInterval, *snapshotOffloadJitter, *snapshotOffloadPublishTimeout
	t.Cleanup(func() {
		*snapshotOffloadInterval, *snapshotOffloadJitter, *snapshotOffloadPublishTimeout = interval, jitter, timeout
	})
	*snapshotOffloadInterval, *snapshotOffloadJitter, *snapshotOffloadPublishTimeout = offloadAcceptancePoll, time.Nanosecond, 5*time.Second
	*snapshotOffloadGCEnabled = false
	*snapshotOffloadGCInterval = offloadAcceptancePoll
	*snapshotOffloadGCPayloadGrace, *snapshotOffloadGCMinMarkAge = 100*time.Millisecond, 100*time.Millisecond
}

func startOffloadAcceptanceWorker(t *testing.T, baseDir string, node *bootstrapE2ENode) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	workers, workerCtx := errgroup.WithContext(ctx)
	require.NoError(t, startSnapshotOffload(workerCtx, workers, node.runtimes, baseDir, node.id, false, nil, testLogger(t)))
	stop := func() {
		cancel()
		require.NoError(t, workers.Wait())
	}
	t.Cleanup(stop)
	return stop
}

func writeOffloadAcceptanceBatch(t *testing.T, client pb.RawKVClient, expected map[string]string, first int) {
	t.Helper()
	for i := first; i < first+offloadAcceptanceBatchSize; i++ {
		key, value := fmt.Sprintf("offload-acceptance-%03d", i), fmt.Sprintf("committed-value-%03d", i)
		require.NoError(t, rawPutWithTimeout(client, []byte(key), []byte(value)))
		expected[key] = value
	}
}

func requireOffloadAcceptanceValues(t *testing.T, client pb.RawKVClient, expected map[string]string) {
	t.Helper()
	// A follower can learn the new leader after the election is visible on
	// the winner. Bound startup convergence rather than assuming it is atomic.
	require.Eventually(t, func() bool {
		for key, value := range expected {
			response, err := rawGetWithTimeout(client, []byte(key), bootstrapE2ERPCRequestTimeout)
			if err != nil || !response.Exists || string(response.Value) != value {
				return false
			}
		}
		return true
	}, offloadAcceptanceTimeout, offloadAcceptancePoll, "adapter did not return every expected value")
}

func requireOffloadAcceptanceLocalValues(t *testing.T, node *bootstrapE2ENode, expected map[string]string) {
	t.Helper()
	// Adapter reads can forward to the leader. Check each replica's local FSM
	// too, so forwarding or a healthy peer cannot hide missing restored data.
	require.Eventually(t, func() bool {
		for key, value := range expected {
			got, err := node.runtimes[0].store.GetAt(context.Background(), []byte(key), math.MaxUint64)
			if err != nil || string(got) != value {
				return false
			}
		}
		return true
	}, offloadAcceptanceTimeout, offloadAcceptancePoll, "%s local FSM did not contain every expected value", node.id)
}

func checkpointOffloadAcceptance(t *testing.T, objects *snapshotoffload.LocalStore, node *bootstrapE2ENode, client pb.RawKVClient, groupID uint64) snapshotoffload.Manifest {
	t.Helper()
	minimum := node.engine().Status().AppliedIndex
	// The fixture has no idle HLC-renewal loop. Advance the real log through
	// the next snapshot threshold so the last asserted user write is covered.
	for range node.engine().SnapshotEvery() {
		require.NoError(t, rawPutWithTimeout(client, []byte("offload-checkpoint"), []byte("barrier")))
	}
	return waitForOffloadAcceptanceManifest(t, objects, groupID, minimum)
}

func waitForOffloadAcceptanceManifest(t *testing.T, objects *snapshotoffload.LocalStore, groupID, minimum uint64) snapshotoffload.Manifest {
	t.Helper()
	var latest snapshotoffload.Manifest
	require.Eventually(t, func() bool {
		refs, err := objects.ListObjects(context.Background(), fmt.Sprintf("cluster-a/v1/groups/%d/snapshots/", groupID))
		if err != nil || len(refs) == 0 {
			return false
		}
		slices.SortFunc(refs, func(a, b snapshotoffload.ObjectRef) int {
			return strings.Compare(b.Key, a.Key)
		})
		latest, err = snapshotoffload.LoadManifest(context.Background(), objects, refs[0].Key)
		return err == nil && latest.SnapshotIndex >= minimum
	}, offloadAcceptanceTimeout, offloadAcceptancePoll, "periodic publication did not reach applied index %d", minimum)
	require.Equal(t, groupID, latest.GroupID)
	require.Equal(t, "cluster-a", latest.SourceCluster)
	return latest
}

func restoreOffloadAcceptanceCluster(t *testing.T, objects *snapshotoffload.LocalStore, manifest snapshotoffload.Manifest, expected map[string]string) {
	t.Helper()
	root := t.TempDir()
	endpoints, listeners := allocateBootstrapE2EEndpoints(t, offloadAcceptanceNodeCount)
	t.Cleanup(func() { closeBootstrapE2EListeners(listeners) })
	peers := make([]etcdraftengine.Peer, len(endpoints))
	for i := range endpoints {
		endpoints[i].id = fmt.Sprintf("replacement-%d", i+1)
		peers[i] = etcdraftengine.Peer{
			NodeID: etcdraftengine.DeriveNodeID(endpoints[i].id), ID: endpoints[i].id, Address: endpoints[i].raftAddr,
			Suffrage: etcdraftengine.SuffrageVoter,
		}
	}
	for _, endpoint := range endpoints {
		dest := groupDataDir(root, endpoint.id, manifest.GroupID, false)
		_, err := os.Stat(dest)
		require.True(t, os.IsNotExist(err))
		result, err := snapshotoffload.RestorePhysicalSnapshot(context.Background(), snapshotoffload.RestoreOptions{
			Store: objects, ManifestKey: manifest.ManifestKey, DataDir: dest, Peers: peers,
			ExpectGroupID: &manifest.GroupID, ExpectSourceCluster: "cluster-a",
		})
		require.NoError(t, err)
		require.Equal(t, manifest.Payload.SHA256, result.PayloadSHA256)
		storedPeers, exists, err := etcdraftengine.LoadPersistedPeers(dest)
		require.NoError(t, err)
		require.True(t, exists)
		require.Len(t, storedPeers, offloadAcceptanceNodeCount)
		for _, peer := range storedPeers {
			require.Contains(t, peers, peer)
		}
	}
	nodes := make([]*bootstrapE2ENode, len(endpoints))
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	for i, endpoint := range endpoints {
		node, err := startBootstrapE2ENode(root, endpoint, listeners[i], false, "", raftEngineEtcd)
		require.NoError(t, err)
		nodes[i] = node
	}
	waitForBootstrapClusterConfig(t, nodes, bootstrapExpectedConfiguration(endpoints))
	leader := waitForSingleLeader(t, nodes)
	clients, conns := rawKVClients(t, endpoints)
	t.Cleanup(func() { closeGRPCConns(conns) })
	for i, node := range nodes {
		require.GreaterOrEqual(t, node.engine().Status().AppliedIndex, manifest.SnapshotIndex)
		reader, ok := node.runtimes[0].stateMachine.(raftengine.AppliedIndexReader)
		require.True(t, ok)
		index, present, err := reader.LastAppliedIndex()
		require.NoError(t, err)
		require.True(t, present)
		require.GreaterOrEqual(t, index, manifest.SnapshotIndex)
		requireOffloadAcceptanceLocalValues(t, node, expected)
		requireOffloadAcceptanceValues(t, clients[i], expected)
		t.Logf("replacement %s: restored index=%d applied=%d durable_applied=%d keys=%d", node.id, manifest.SnapshotIndex, node.engine().Status().AppliedIndex, index, len(expected))
	}
	require.NoError(t, rawPutWithTimeout(clients[leader], []byte("after-offload-restore"), []byte("replicated")))
	committed := nodes[leader].engine().Status().AppliedIndex
	expected["after-offload-restore"] = "replicated"
	for i, node := range nodes {
		requireOffloadAcceptanceLocalValues(t, node, expected)
		requireOffloadAcceptanceValues(t, clients[i], expected)
		require.Eventually(t, func() bool {
			return node.engine().Status().AppliedIndex >= committed
		}, offloadAcceptanceTimeout, offloadAcceptancePoll)
	}
	t.Logf("recovery: %d nodes read all %d keys and accepted a new replicated write", len(nodes), len(expected))
}
