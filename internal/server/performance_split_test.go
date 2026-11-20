package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/serverwiring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestPerformanceReadWriteSplit is an opt-in local diagnostic, not a throughput
// assertion. Three Raft nodes share this process and disk. Requests use TLS and
// one fixed ingress; post-migration forwarding is included in the measurement.
func TestPerformanceReadWriteSplit(t *testing.T) {
	if os.Getenv("ELASTICKV_PERF_SPLIT") != "1" {
		t.Skip("set ELASTICKV_PERF_SPLIT=1 to measure read/write and split performance")
	}
	flags := enableMigrationRuntimeFlags(t)
	cert, certKey := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, certKey))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := "local-performance-token"
	// Match startDistributionStartup: authenticated Forward needs the dedicated
	// migration token even when the test bypasses production service startup.
	internalutil.ConfigureGRPCForwardToken(token)
	t.Cleanup(func() { internalutil.ConfigureGRPCForwardToken("") })
	flags.SplitMigrationTokenFile = filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(flags.SplitMigrationTokenFile, []byte(token), 0o600))
	_, auth, err := configureAdminService(flags, false, adapter.NodeIdentity{}, nil, nil)
	require.NoError(t, err)
	groupIDs := []uint64{1, 2}
	endpoints, listeners := allocateBootstrapE2EMultiGroupEndpoints(t, 3, groupIDs)
	peers := bootstrapGroupPeersArg(endpoints, groupIDs)
	var nodes []*bootstrapE2ENode
	t.Cleanup(func() { closeBootstrapE2ENodes(t, nodes) })
	for i := range endpoints {
		node, startErr := startBootstrapE2EMultiGroupNode(t.TempDir(), endpoints[i], listeners[i], true,
			groupIDs, peers, noderuntime.RaftEngineEtcd, bootstrapE2EMigrationConfig{flags: flags, auth: auth, token: token})
		require.NoError(t, startErr)
		nodes = append(nodes, node)
	}
	waitForMultiGroupBootstrapClusterConfig(t, nodes, endpoints, groupIDs, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	pinPerformanceSplitLeaders(t, nodes, endpoints)
	leader := waitForGroupLeader(t, nodes, 1, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	targetLeader := waitForGroupLeader(t, nodes, 2, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	t.Logf("PERF topology nodes=3 groups=2 source_leader=%d target_leader=%d fsm_sync_mode=%q", leader, targetLeader, os.Getenv("ELASTICKV_FSM_SYNC_MODE"))
	assertBootstrapSplitMigrationCapabilities(t, endpoints, groupIDs)
	conn, err := grpc.NewClient(endpoints[leader].raftAddrs[1], internalutil.GRPCDialOptions()...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := pb.NewRawKVClient(conn)
	value := bytes.Repeat([]byte("v"), 1024)
	keys := make([][]byte, 256)
	for i := range keys {
		prefix := "a"
		if i%2 == 1 {
			prefix = "z"
		}
		keys[i] = fmt.Appendf(nil, "%s-%06d", prefix, i)
		require.NoError(t, rawPutWithTimeout(client, keys[i], value))
	}
	measure := func(name string, readers, writers int) {
		done := make(chan struct{})
		timer := time.AfterFunc(2*time.Second, func() { close(done) })
		defer timer.Stop()
		measureSplitTraffic(t, client, keys, value, name, readers, writers, done, true)
	}
	measure("before-read", 8, 0)
	measure("before-write", 0, 8)
	measure("before-mixed", 4, 4)
	snapshot, err := nodes[leader].distCatalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Len(t, snapshot.Routes, 1)
	splitter := serverwiring.DistributionSplitter{Server: nodes[leader].distServer}
	start := time.Now()
	same, err := splitter.SplitRange(t.Context(), autosplit.SplitRequest{
		ExpectedCatalogVersion: snapshot.Version, RouteID: snapshot.Routes[0].RouteID, SplitKey: []byte("m"),
	})
	require.NoError(t, err)
	t.Logf("PERF same-group catalog_commit_ms=%.3f", float64(time.Since(start).Microseconds())/1000)
	waitForCatalogVersion(t, nodes, same.CatalogVersion, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	measure("same-group-mixed", 4, 4)

	// Include start, copy, fence, cutover, promotion, read drain and cleanup.
	done := make(chan struct{})
	trafficDone := make(chan struct{})
	phases := newSplitPhaseRecorder()
	go func() {
		defer close(trafficDone)
		measureSplitTraffic(t, client, keys, value, "cross-group-during", 4, 4, done, false, phases)
	}()
	start = time.Now()
	result, splitErr := splitter.SplitRange(t.Context(), autosplit.SplitRequest{
		ExpectedCatalogVersion: same.CatalogVersion, RouteID: same.Right.RouteID,
		SplitKey: []byte("t"), TargetGroupID: 2,
	})
	completed, lastPhase, lastError := false, distribution.SplitJobPhaseNone, ""
	if splitErr == nil {
		stopWatch := watchPerformancePreflight(t, nodes[leader].distCatalog, result.JobID)
		completed, lastPhase, lastError = waitPerformanceSplitJob(t, nodes[leader].distCatalog, result.JobID, start, phases)
		stopWatch()
	}
	close(done)
	<-trafficDone
	t.Logf("PERF native-partition-runtime source=%t target=%t", nodes[leader].distServer.MigrationPartitionRuntimeConfigured(1), nodes[leader].distServer.MigrationPartitionRuntimeConfigured(2))
	require.NoError(t, splitErr)
	require.True(t, completed, "phase=%d last_error=%q", lastPhase, lastError)
	if os.Getenv("ELASTICKV_PERF_PREFLIGHT_ONLY") == "1" {
		require.Equal(t, distribution.SplitJobPhaseFence, lastPhase)
		t.Logf("PERF preflight-pilot stopped_after_witness=true elapsed_ms=%d", time.Since(start).Milliseconds())
		return
	}
	t.Logf("PERF cross-group done_ms=%d", time.Since(start).Milliseconds())
	measure("after-mixed", 4, 4)
	measure("after-read", 8, 0)
	measure("after-write", 0, 8)
	for _, key := range keys {
		got, getErr := rawGetWithTimeout(client, key, bootstrapE2ERPCRequestTimeout)
		require.NoError(t, getErr)
		require.True(t, got.Exists)
		require.Equal(t, value, got.Value)
	}
	verifySplitWriteRecovery(t, nodes, endpoints)
}

type splitTrafficResult struct {
	write     bool
	latencies []time.Duration
	errors    int
	firstErr  error
	phases    map[uint32]*splitTrafficResult
}

func measureSplitTraffic(t *testing.T, client pb.RawKVClient, keys [][]byte, value []byte,
	name string, readers, writers int, done <-chan struct{}, requireSuccess bool, recorders ...*splitPhaseRecorder,
) {
	t.Helper()
	results := make([]splitTrafficResult, readers+writers)
	var wg sync.WaitGroup
	start := time.Now()
	for worker := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runSplitTrafficWorker(client, keys, value, worker, readers, done, results, recorders...)
		}()
	}
	wg.Wait()
	if len(recorders) > 0 {
		reportSplitPhaseTraffic(t, results, recorders[0])
	}
	elapsed := time.Since(start).Seconds()
	for _, write := range []bool{false, true} {
		reportSplitTraffic(t, results, name, write, elapsed, requireSuccess)
	}
}

func reportSplitTraffic(t *testing.T, results []splitTrafficResult, name string, write bool, elapsed float64, requireSuccess bool) {
	t.Helper()
	var latencies []time.Duration
	errors := 0
	var firstErr error
	for _, r := range results {
		if r.write == write {
			latencies = append(latencies, r.latencies...)
			errors += r.errors
			if firstErr == nil {
				firstErr = r.firstErr
			}
		}
	}
	if len(latencies) == 0 && errors == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p int) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return float64(latencies[(len(latencies)-1)*p/100].Microseconds()) / 1000
	}
	t.Logf("PERF phase=%s write=%t elapsed_s=%.3f success=%d errors=%d ops_s=%.1f p50_ms=%.3f p99_ms=%.3f first_error=%v",
		name, write, elapsed, len(latencies), errors, float64(len(latencies))/elapsed, percentile(50), percentile(99), firstErr)
	if requireSuccess {
		if errors != 0 {
			t.Errorf("%s write=%t errors=%d: %v", name, write, errors, firstErr)
		}
	}
}

func runSplitTrafficWorker(client pb.RawKVClient, keys [][]byte, value []byte, worker, readers int, done <-chan struct{}, results []splitTrafficResult, recorders ...*splitPhaseRecorder) {
	r := &results[worker]
	r.write = worker >= readers
	r.phases = make(map[uint32]*splitTrafficResult)
	for n := 0; ; n++ {
		select {
		case <-done:
			return
		default:
		}
		key := keys[(n*len(results)+worker)%len(keys)]
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var phase uint32
		if len(recorders) > 0 {
			phase = recorders[0].current.Load()
		}
		opStart := time.Now()
		err := splitTrafficOperation(ctx, client, key, value, r.write)
		cancel()
		latency := time.Since(opStart)
		r.recordOperation(err, latency)
		if len(recorders) > 0 {
			r.recordPhaseOperation(phase, err, latency)
		}
	}
}

func splitTrafficOperation(ctx context.Context, client pb.RawKVClient, key, value []byte, write bool) error {
	if write {
		resp, err := client.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: value})
		if err != nil {
			return err
		}
		if !resp.GetSuccess() {
			return fmt.Errorf("put unsuccessful")
		}
		return nil
	}
	resp, err := client.RawGet(ctx, &pb.RawGetRequest{Key: key})
	if err != nil {
		return err
	}
	if !resp.GetExists() || !bytes.Equal(resp.GetValue(), value) {
		return fmt.Errorf("read mismatch for %q", key)
	}
	return nil
}

func waitPerformanceSplitJob(t *testing.T, catalog *distribution.CatalogStore, jobID uint64, start time.Time, recorders ...*splitPhaseRecorder) (bool, distribution.SplitJobPhase, string) {
	t.Helper()
	lastPhase, lastError := distribution.SplitJobPhaseNone, ""
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		job, found, err := catalog.SplitJob(t.Context(), jobID)
		if err == nil && found {
			if len(recorders) > 0 {
				recorders[0].observe(job)
			}
			if job.Phase != lastPhase {
				t.Logf("PERF migration elapsed_ms=%d phase=%d last_error=%q", time.Since(start).Milliseconds(), job.Phase, job.LastError)
			}
			lastPhase, lastError = job.Phase, job.LastError
			if job.Phase == distribution.SplitJobPhaseDone || (os.Getenv("ELASTICKV_PERF_PREFLIGHT_ONLY") == "1" && job.TransferPreflightDone) {
				return true, lastPhase, lastError
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false, lastPhase, lastError
}
