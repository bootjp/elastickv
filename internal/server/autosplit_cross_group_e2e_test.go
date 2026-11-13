package server

import (
	"context"
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
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestAutoSplitCrossGroupThreeNodeTwoGroupAcceptance(t *testing.T) {
	flags := enableMigrationRuntimeFlags(t)
	cert, certKey := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, certKey))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := "autosplit-cross-group-acceptance-token"
	tokenPath := filepath.Join(t.TempDir(), "migration-token")
	require.NoError(t, os.WriteFile(tokenPath, []byte(token), 0o600))
	flags.SplitMigrationTokenFile = tokenPath
	_, auth, err := configureAdminService(flags, false, adapter.NodeIdentity{}, nil, nil)
	require.NoError(t, err)
	migration := bootstrapE2EMigrationConfig{flags: flags, auth: auth, token: token}
	groupIDs := []uint64{1, 2}
	endpoints, listeners := allocateBootstrapE2EMultiGroupEndpoints(t, 3, groupIDs)
	peers := bootstrapGroupPeersArg(endpoints, groupIDs)
	nodes := make([]*bootstrapE2ENode, 0, len(endpoints))
	for i := range endpoints {
		node, startErr := startBootstrapE2EMultiGroupNode(t.TempDir(), endpoints[i], listeners[i], true,
			groupIDs, peers, noderuntime.RaftEngineEtcd, migration)
		require.NoError(t, startErr)
		nodes = append(nodes, node)
	}
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	waitForMultiGroupBootstrapClusterConfig(t, nodes, endpoints, groupIDs, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	leader := waitForGroupLeader(t, nodes, 1, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	assertBootstrapSplitMigrationCapabilities(t, endpoints, groupIDs)

	conn, err := grpc.NewClient(endpoints[leader].raftAddrs[1], internalutil.GRPCDialOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := pb.NewRawKVClient(conn)
	key, value := []byte{0xff, 'm', 'i', 'g', 'r', 'a', 't', 'e', 'd'}, []byte("migrated-value")
	require.Eventually(t, func() bool { return rawPutWithTimeout(client, key, value) == nil },
		bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	snapshot, err := nodes[leader].distCatalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snapshot.Routes, 1)
	require.Equal(t, uint64(1), snapshot.Routes[0].GroupID)

	base := time.Now().Truncate(time.Second)
	clock := &autoSplitE2EClock{}
	clock.Set(base)
	sampler := keyviz.NewMemSampler(keyviz.MemSamplerOptions{
		Step: time.Minute, HistoryColumns: 32, MaxTrackedRoutes: 64,
		KeyBucketsPerRoute: 16, Now: clock.Now,
	})
	nodes[leader].coordinate.WithSampler(sampler)
	cfg := autosplit.SchedulerConfig{
		Enabled: true, CrossGroupEnabled: true, CandidateGroupIDs: groupIDs,
		EvalInterval: time.Minute, SplitCooldown: time.Millisecond, SplitTimeout: 5 * time.Second,
		Detector: autosplit.Config{
			WriteWeight: 1, ThresholdOpsMin: 1, CandidateWindows: 2,
			MaxRoutes: 64, MaxSplitsPerCycle: 1,
		},
		Reconciler: autosplit.NewRouteReconciler(sampler),
	}
	serverwiring.ApplyLeadership(&cfg, nodes[leader].coordinate)
	scheduler := autosplit.NewScheduler(cfg, nodes[leader].distCatalog,
		serverwiring.DistributionSplitter{Server: nodes[leader].distServer}, sampler, sampler)
	initial, err := scheduler.Tick(t.Context(), base)
	require.NoError(t, err)
	require.True(t, initial.Leader)

	// Public ingress is already serving. The first flush opens registered route
	// membership; enrollment and its transition column cannot prove read coverage.
	clock.Set(base.Add(time.Minute))
	publishLeaderTerms(sampler, nodes[leader].runtimes)
	sampler.Flush()
	enrollKeyVizReadCoverage(sampler, true)
	clock.Set(base.Add(2 * time.Minute))
	sampler.Flush()
	runCrossGroupAutoSplitWindows(t, client, nodes[leader], scheduler, sampler, clock, snapshot.Routes[0], base, key, value)

	t.Cleanup(func() { logSplitMigrationAcceptanceFailure(t, nodes) })
	var jobID uint64
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		jobs, listErr := nodes[leader].distCatalog.ListSplitJobs(context.Background())
		require.NoError(c, listErr)
		require.Len(c, jobs, 1)
		jobID = jobs[0].JobID
		require.Equal(c, uint64(2), jobs[0].TargetGroupID)
		require.Equal(c, snapshot.Routes[0].RouteID, jobs[0].SourceRouteID)
		require.Equal(c, distribution.SplitJobPhaseDone, jobs[0].Phase,
			"job %d phase=%d branch=%s last_error=%q", jobID, jobs[0].Phase,
			splitMigrationAcceptanceCleanupBranch(jobs[0]), jobs[0].LastError)
	}, 5*time.Minute, 200*time.Millisecond)
	require.NotZero(t, jobID)

	assertBootstrapSplitMigrationDataMoved(t, nodes, key, value)
	got, err := rawGetWithTimeout(client, key, bootstrapE2ERPCRequestTimeout)
	require.NoError(t, err)
	require.True(t, got.Exists)
	require.Equal(t, value, got.Value)
}

func runCrossGroupAutoSplitWindows(
	t *testing.T, client pb.RawKVClient, node *bootstrapE2ENode, scheduler *autosplit.Scheduler,
	sampler *keyviz.MemSampler, clock *autoSplitE2EClock, source distribution.RouteDescriptor,
	base time.Time, key, value []byte,
) {
	t.Helper()
	for minute := 3; minute <= 4; minute++ {
		for i := range 40 {
			writeKey := key
			if i < 10 {
				writeKey = []byte("a-source-key")
			}
			require.NoError(t, rawPutWithTimeout(client, writeKey, value))
		}
		at := base.Add(time.Duration(minute) * time.Minute)
		clock.Set(at)
		publishLeaderTerms(sampler, node.runtimes)
		sampler.Flush()
		cols := sampler.Snapshot(at.Add(-time.Minute), at.Add(time.Nanosecond))
		require.NotEmpty(t, cols)
		col := cols[len(cols)-1]
		require.True(t, sampler.ReadCoverageCoversWindow(source.RouteID, col.WindowStart, col.At))
		require.True(t, sampler.RouteRegistrationCoversWindow(source.RouteID,
			1, source.Start, source.End, col.WindowStart, col.At))
		for _, row := range col.Rows {
			if row.RouteID == source.RouteID {
				require.NotZero(t, row.LeaderTerm)
			}
		}
		result, tickErr := scheduler.Tick(t.Context(), at)
		require.NoError(t, tickErr)
		require.True(t, result.Leader)
		if minute == 3 {
			require.Zero(t, result.Scheduled)
			continue
		}
		require.Len(t, result.Detector.Decisions, 1)
		require.Equal(t, uint64(2), result.Detector.Decisions[0].TargetGroupID,
			"decision=%+v rows=%+v", result.Detector.Decisions[0], col.Rows)
		require.Equal(t, 1, result.Scheduled, "decisions=%+v failed=%d", result.Detector.Decisions, result.Failed)
	}
}
