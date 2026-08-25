package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestAutoSplitE2EThreeNodeSplitKillSwitchAndLeadershipReset(t *testing.T) {
	const (
		nodeCount    = 3
		waitTimeout  = 20 * time.Second
		waitInterval = 100 * time.Millisecond
		rpcTimeout   = 2 * time.Second
	)
	baseDir := t.TempDir()
	_, endpoints, nodes := startBootstrapE2ECluster(t, baseDir, nodeCount, 5, raftEngineEtcd)
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	waitForBootstrapClusterConfig(t, nodes, bootstrapExpectedConfiguration(endpoints))
	leaderIdx := waitForSingleLeader(t, nodes)
	clients, conns := rawKVClients(t, endpoints)
	t.Cleanup(func() { closeGRPCConns(conns) })

	base := time.Now().Truncate(time.Second)
	leaderClock, leaderSampler, leaderRuntime, leaderScheduler := newAutoSplitE2EScheduler(t, nodes[leaderIdx], base)
	first, err := leaderScheduler.Tick(context.Background(), base)
	require.NoError(t, err)
	require.True(t, first.Leader)
	initialVersion := first.CatalogVersion

	writeAutoSplitWindow(t, clients[leaderIdx], leaderSampler, leaderClock, base.Add(time.Minute), []byte("a"), []byte("z"))
	writeAutoSplitWindow(t, clients[leaderIdx], leaderSampler, leaderClock, base.Add(2*time.Minute), []byte("a"), []byte("z"))
	result, err := leaderScheduler.Tick(context.Background(), base.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, result.Scheduled)
	require.Greater(t, result.CatalogVersion, initialVersion)
	firstSplitVersion := currentCatalogVersion(t, nodes[leaderIdx])
	require.Greater(t, firstSplitVersion, initialVersion)
	waitForCatalogVersion(t, nodes, firstSplitVersion, waitTimeout, waitInterval)

	_, err = leaderScheduler.Tick(context.Background(), base.Add(2*time.Minute+time.Second))
	require.NoError(t, err)
	leaderRuntime.SetEnabled(false)
	writeAutoSplitWindow(t, clients[leaderIdx], leaderSampler, leaderClock, base.Add(3*time.Minute), []byte("z"), []byte("zz"))
	writeAutoSplitWindow(t, clients[leaderIdx], leaderSampler, leaderClock, base.Add(4*time.Minute), []byte("z"), []byte("zz"))
	blocked, err := leaderScheduler.Tick(context.Background(), base.Add(4*time.Minute))
	require.NoError(t, err)
	require.True(t, blocked.KillSwitch)
	require.Equal(t, firstSplitVersion, currentCatalogVersion(t, nodes[leaderIdx]))

	leaderRuntime.SetEnabled(true)
	writeAutoSplitWindow(t, clients[leaderIdx], leaderSampler, leaderClock, base.Add(5*time.Minute), []byte("z"), []byte("zz"))
	reenabled, err := leaderScheduler.Tick(context.Background(), base.Add(5*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, reenabled.Scheduled)
	secondSplitVersion := currentCatalogVersion(t, nodes[leaderIdx])
	require.Greater(t, secondSplitVersion, firstSplitVersion)
	waitForCatalogVersion(t, nodes, secondSplitVersion, waitTimeout, waitInterval)

	targetIdx := (leaderIdx + 1) % len(nodes)
	targetClock, targetSampler, _, targetScheduler := newAutoSplitE2EScheduler(t, nodes[targetIdx], base.Add(5*time.Minute))
	followerTick, err := targetScheduler.Tick(context.Background(), base.Add(5*time.Minute))
	require.NoError(t, err)
	require.False(t, followerTick.Leader)
	writeAutoSplitWindow(t, clients[targetIdx], targetSampler, targetClock, base.Add(6*time.Minute), []byte("a"), []byte("aa"))
	writeAutoSplitWindow(t, clients[targetIdx], targetSampler, targetClock, base.Add(7*time.Minute), []byte("a"), []byte("aa"))

	admin, ok := nodes[leaderIdx].engine().(raftengine.Admin)
	require.True(t, ok)
	transferCtx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	err = admin.TransferLeadershipToServer(transferCtx, endpoints[targetIdx].id, endpoints[targetIdx].raftAddr)
	cancel()
	require.NoError(t, err)
	waitForGroupLeaderOnNode(t, nodes, 1, targetIdx, waitTimeout, waitInterval)

	postTransfer, err := targetScheduler.Tick(context.Background(), base.Add(7*time.Minute+30*time.Second))
	require.NoError(t, err)
	require.True(t, postTransfer.Leader)
	require.Empty(t, postTransfer.Detector.Decisions)
	require.Equal(t, secondSplitVersion, currentCatalogVersion(t, nodes[targetIdx]))

	writeAutoSplitWindow(t, clients[targetIdx], targetSampler, targetClock, base.Add(8*time.Minute), []byte("a"), []byte("aa"))
	writeAutoSplitWindow(t, clients[targetIdx], targetSampler, targetClock, base.Add(9*time.Minute), []byte("a"), []byte("aa"))
	notYet, err := targetScheduler.Tick(context.Background(), base.Add(9*time.Minute))
	require.NoError(t, err)
	require.Empty(t, notYet.Detector.Decisions)
	writeAutoSplitWindow(t, clients[targetIdx], targetSampler, targetClock, base.Add(10*time.Minute), []byte("a"), []byte("aa"))
	reearned, err := targetScheduler.Tick(context.Background(), base.Add(10*time.Minute))
	require.NoError(t, err)
	require.Equal(t, 1, reearned.Scheduled)
	require.Greater(t, currentCatalogVersion(t, nodes[targetIdx]), secondSplitVersion)
}

type autoSplitE2EClock struct {
	unixMilli atomic.Int64
}

func (c *autoSplitE2EClock) Now() time.Time {
	return time.UnixMilli(c.unixMilli.Load())
}

func (c *autoSplitE2EClock) Set(now time.Time) {
	c.unixMilli.Store(now.UnixMilli())
}

func newAutoSplitE2EScheduler(
	t *testing.T,
	node *bootstrapE2ENode,
	now time.Time,
) (*autoSplitE2EClock, *keyviz.MemSampler, *autosplit.RuntimeSwitch, *autosplit.Scheduler) {
	t.Helper()
	clock := &autoSplitE2EClock{}
	clock.Set(now)
	sampler := keyviz.NewMemSampler(keyviz.MemSamplerOptions{
		Step:               time.Minute,
		HistoryColumns:     32,
		MaxTrackedRoutes:   64,
		KeyBucketsPerRoute: 16,
		Now:                clock.Now,
	})
	snapshot, err := node.distCatalog.Snapshot(context.Background())
	require.NoError(t, err)
	for _, route := range snapshot.Routes {
		sampler.RegisterRoute(route.RouteID, route.Start, route.End, route.GroupID)
	}
	node.coordinate.WithSampler(sampler)
	runtime := autosplit.NewRuntimeSwitch(true)
	cfg := autosplit.SchedulerConfig{
		Enabled:       true,
		EvalInterval:  time.Minute,
		SplitCooldown: time.Millisecond,
		SplitTimeout:  5 * time.Second,
		KillSwitch:    runtime.KillSwitch,
		Detector: autosplit.Config{
			WriteWeight:       1,
			ThresholdOpsMin:   1,
			CandidateWindows:  2,
			MaxRoutes:         64,
			MaxSplitsPerCycle: 1,
		},
		Reconciler: autosplit.NewRouteReconciler(sampler),
	}
	applyAutoSplitLeadership(&cfg, node.coordinate)
	return clock, sampler, runtime, autosplit.NewScheduler(
		cfg,
		node.distCatalog,
		autoSplitDistributionSplitter{server: node.distServer},
		sampler,
		sampler,
	)
}

func writeAutoSplitWindow(
	t *testing.T,
	client pb.RawKVClient,
	sampler *keyviz.MemSampler,
	clock *autoSplitE2EClock,
	at time.Time,
	keys ...[]byte,
) {
	t.Helper()
	for i := range 40 {
		key := keys[i%len(keys)]
		require.NoError(t, rawPutWithTimeout(client, key, []byte("value")))
	}
	clock.Set(at)
	sampler.Flush()
}

func currentCatalogVersion(t *testing.T, node *bootstrapE2ENode) uint64 {
	t.Helper()
	snapshot, err := node.distCatalog.Snapshot(context.Background())
	require.NoError(t, err)
	return snapshot.Version
}

func waitForCatalogVersion(
	t *testing.T,
	nodes []*bootstrapE2ENode,
	version uint64,
	timeout time.Duration,
	interval time.Duration,
) {
	t.Helper()
	for _, node := range nodes {
		require.Eventually(t, func() bool {
			snapshot, err := node.distCatalog.Snapshot(context.Background())
			return err == nil && snapshot.Version >= version && catalogRoutesActive(snapshot.Routes)
		}, timeout, interval)
	}
}

func catalogRoutesActive(routes []distribution.RouteDescriptor) bool {
	if len(routes) < 2 {
		return false
	}
	for _, route := range routes {
		if route.State != distribution.RouteStateActive {
			return false
		}
	}
	return true
}
