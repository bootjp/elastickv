package server

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/serverwiring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSplitMigrationThreeNodeTwoGroupAcceptance(t *testing.T) {
	flags := enableMigrationRuntimeFlags(t)
	cert, certKey := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, certKey))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := "split-migration-acceptance-token"
	path := filepath.Join(t.TempDir(), "migration-token")
	require.NoError(t, os.WriteFile(path, []byte(token), 0o600))
	flags.SplitMigrationTokenFile = path
	_, auth, err := configureAdminService(flags, false, adapter.NodeIdentity{}, nil, nil)
	require.NoError(t, err)
	migration := bootstrapE2EMigrationConfig{flags: flags, auth: auth, token: token}
	groupIDs := []uint64{1, 2}
	endpoints, listeners := allocateBootstrapE2EMultiGroupEndpoints(t, 3, groupIDs)
	groupPeers := bootstrapGroupPeersArg(endpoints, groupIDs)
	nodes := make([]*bootstrapE2ENode, 0, len(endpoints))
	for i := range endpoints {
		node, startErr := startBootstrapE2EMultiGroupNode(t.TempDir(), endpoints[i], listeners[i], true, groupIDs, groupPeers, noderuntime.RaftEngineEtcd, migration)
		require.NoError(t, startErr)
		nodes = append(nodes, node)
	}
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	waitForMultiGroupBootstrapClusterConfig(t, nodes, endpoints, groupIDs, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	leader := waitForGroupLeader(t, nodes, 1, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)

	assertBootstrapSplitMigrationCapabilities(t, endpoints, groupIDs)

	clients := make([]pb.RawKVClient, 0, len(endpoints))
	conns := make([]*grpc.ClientConn, 0, len(endpoints))
	for _, endpoint := range endpoints {
		conn, dialErr := grpc.NewClient(endpoint.raftAddrs[1], internalutil.GRPCDialOptions()...)
		require.NoError(t, dialErr)
		conns = append(conns, conn)
		clients = append(clients, pb.NewRawKVClient(conn))
	}
	t.Cleanup(func() { closeGRPCConns(conns) })
	key, value := []byte("z-migrated-key"), []byte("migrated-value")
	require.Eventually(t, func() bool { return rawPutWithTimeout(clients[leader], key, value) == nil }, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	snapshot, err := nodes[leader].distCatalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snapshot.Routes, 1)
	result, err := (serverwiring.DistributionSplitter{Server: nodes[leader].distServer}).SplitRange(t.Context(), autosplit.SplitRequest{
		ExpectedCatalogVersion: snapshot.Version, RouteID: snapshot.Routes[0].RouteID,
		SplitKey: []byte("m"), TargetGroupID: 2,
	})
	require.NoError(t, err)
	require.NotZero(t, result.JobID)
	t.Cleanup(func() { logSplitMigrationAcceptanceFailure(t, nodes) })
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		jobs, getErr := nodes[leader].distCatalog.ListSplitJobs(context.Background())
		require.NoError(c, getErr)
		for _, job := range jobs {
			if job.JobID == result.JobID {
				require.Equal(c, distribution.SplitJobPhaseDone, job.Phase,
					"job %d phase=%d branch=%s cutover_version=%d fence_ts=%d split_key=%x preflight=%d/%d promotion=%t promotion_ts=%d target_ack=%x source_ack=%x read_drain=%x source_cutover_read_fence_ack=%x target_staged_readiness_ack=%x cleanup_cursor=%x retention_pin=%d updated=%d last_error=%q", job.JobID, job.Phase, splitMigrationAcceptanceCleanupBranch(job), job.CutoverVersion, job.FenceTS, job.SplitKey,
					countCompletedMigrationBrackets(job.TransferPreflightProgress), len(job.TransferPreflightProgress), job.TargetPromotionDone,
					job.PromotionCompletedTS, job.TargetClearedDescriptorAckCursor, job.SourceCutoverAckCursor, job.SourceReadDrainCursor,
					job.SourceCutoverReadFenceAckCursor, job.TargetStagedReadinessAckCursor, job.Cursor, job.SourceRetentionPinTS, job.UpdatedAtMs, job.LastError)
				return
			}
		}
		require.Fail(c, "split job missing", "job %d not in %+v", result.JobID, jobs)
	}, 5*time.Minute, 200*time.Millisecond)
	assertBootstrapSplitMigrationDataMoved(t, nodes, key, value)
	got, err := rawGetWithTimeout(clients[leader], key, bootstrapE2ERPCRequestTimeout)
	require.NoError(t, err)
	require.True(t, got.Exists)
	require.Equal(t, value, got.Value)
}

func assertBootstrapSplitMigrationCapabilities(t *testing.T, endpoints []bootstrapE2EMultiGroupEndpoint, groupIDs []uint64) {
	t.Helper()
	for _, endpoint := range endpoints {
		for _, groupID := range groupIDs {
			conn, dialErr := grpc.NewClient(endpoint.raftAddrs[groupID], internalutil.GRPCDialOptions()...)
			require.NoError(t, dialErr)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				capability, probeErr := pb.NewDistributionClient(conn).GetSplitMigrationCapability(ctx, &pb.GetSplitMigrationCapabilityRequest{})
				require.NoError(c, probeErr)
				require.True(c, capability.GetMigrationCapable(), "peer %s group %d capability: %+v", endpoint.id, groupID, capability)
			}, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
		}
	}
}

func logSplitMigrationAcceptanceFailure(t *testing.T, nodes []*bootstrapE2ENode) {
	t.Helper()
	if !t.Failed() {
		return
	}
	for _, node := range nodes {
		route, found := node.distEngine.GetRoute([]byte("m"))
		t.Logf("node=%s target_route_found=%t target_route=%+v", node.id, found, route)
		for _, rt := range node.runtimes {
			if rt.Spec.ID != 1 {
				continue
			}
			version, versionErr := rt.Store.GetAt(context.Background(), distribution.CatalogVersionKey(), math.MaxUint64)
			floor, floorErr := rt.Store.GetAt(context.Background(), distribution.CatalogDeltaFloorKey(), math.MaxUint64)
			delta, deltaErr := rt.Store.GetAt(context.Background(), distribution.CatalogDeltaKey(3), math.MaxUint64)
			t.Logf("node=%s engine_version=%d catalog_version=%x version_err=%v delta_floor=%x floor_err=%v delta_3=%x delta_err=%v", node.id, node.distEngine.Version(), version, versionErr, floor, floorErr, delta, deltaErr)
		}
	}
}

func assertBootstrapSplitMigrationDataMoved(t *testing.T, nodes []*bootstrapE2ENode, key, value []byte) {
	t.Helper()
	for _, node := range nodes {
		for _, rt := range node.runtimes {
			got, getErr := rt.Store.GetAt(t.Context(), key, math.MaxUint64)
			if rt.Spec.ID == 2 {
				require.NoError(t, getErr)
				require.Equal(t, value, got)
			} else {
				require.ErrorIs(t, getErr, store.ErrKeyNotFound)
			}
		}
	}
}

func splitMigrationAcceptanceCleanupBranch(job distribution.SplitJob) string {
	if job.Phase != distribution.SplitJobPhaseCleanup {
		return "not in cleanup"
	}
	if !job.TargetPromotionDone {
		return "target ownership acquire/promotion"
	}
	if !bytes.Equal(job.TargetClearedDescriptorAckCursor, []byte{1, 0xff, 0xff, 0xff, 0xff}) {
		return "target descriptor/metadata clear"
	}
	if !bytes.Equal(job.SourceCutoverAckCursor, []byte{1, 0xff, 0xff, 0xff, 0xff}) ||
		!bytes.Equal(job.SourceReadDrainCursor, []byte{1, 0xff, 0xff, 0xff, 0xff}) {
		return "source route-removal/read-drain voter barrier"
	}
	if !bytes.Equal(job.Cursor, []byte{2, 0xff, 0xff, 0xff, 0xff}) {
		return "source version cleanup"
	}
	if job.SourceRetentionPinTS != math.MaxUint64 {
		return "source metadata clear/retention release"
	}
	return "terminal history write"
}

func countCompletedMigrationBrackets(progress []distribution.SplitJobBracketProgress) int {
	count := 0
	for _, bracket := range progress {
		if bracket.Done {
			count++
		}
	}
	return count
}
