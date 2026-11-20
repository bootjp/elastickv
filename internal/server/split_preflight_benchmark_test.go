package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/serverwiring"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// BenchmarkSplitPreflight creates a fresh three-node, two-group cluster per
// iteration. Run with -benchtime=1x. The timer spans the first observed Fence
// state through the durable TransferPreflightDone witness. The foreground
// client issues one retained-range write and read per scheduled interval.
func BenchmarkSplitPreflight(b *testing.B) {
	b.StopTimer()
	flags := enableMigrationRuntimeFlags(b)
	cert, key := writeSelfSignedCert(b)
	require.NoError(b, internalutil.ConfigureGRPCTLS(cert, cert, key))
	b.Cleanup(func() { require.NoError(b, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := b.Name()
	internalutil.ConfigureGRPCForwardToken(token)
	b.Cleanup(func() { internalutil.ConfigureGRPCForwardToken("") })
	flags.SplitMigrationTokenFile = filepath.Join(b.TempDir(), "peer-token")
	require.NoError(b, os.WriteFile(flags.SplitMigrationTokenFile, []byte(token), 0o600))
	_, auth, err := configureAdminService(flags, false, adapter.NodeIdentity{}, nil, nil)
	require.NoError(b, err)
	migration := bootstrapE2EMigrationConfig{flags: flags, auth: auth, token: token}
	var samples preflightBenchSamples
	for range b.N {
		runPreflightBenchmark(b, migration, &samples)
	}
	b.ReportMetric(float64(len(samples.reads))/float64(b.N), "reads/op")
	b.ReportMetric(float64(len(samples.writes))/float64(b.N), "writes/op")
	b.ReportMetric(float64(samples.missed)/float64(b.N), "missed-slots/op")
	b.ReportMetric(preflightP99Millis(samples.reads), "read-p99-ms")
	b.ReportMetric(preflightP99Millis(samples.writes), "write-p99-ms")
	b.ReportMetric(float64(samples.maxStartLag)/float64(time.Millisecond), "max-start-lag-ms")
}

type preflightBenchSamples struct {
	reads       []time.Duration
	writes      []time.Duration
	maxStartLag time.Duration
	missed      int
}

func preflightP99Millis(durations []time.Duration) float64 {
	slices.Sort(durations)
	return float64(durations[len(durations)-1-len(durations)/100]) / float64(time.Millisecond)
}

func runPreflightBenchmark(b *testing.B, migration bootstrapE2EMigrationConfig, samples *preflightBenchSamples) {
	b.Helper()
	groups := []uint64{1, 2}
	endpoints, listeners := allocateBootstrapE2EMultiGroupEndpoints(b, 3, groups)
	peers := bootstrapGroupPeersArg(endpoints, groups)
	var nodes []*bootstrapE2ENode
	defer func() { closeBootstrapE2ENodes(b, nodes) }()
	for i := range endpoints {
		node, err := startBootstrapE2EMultiGroupNode(b.TempDir(), endpoints[i], listeners[i], true,
			groups, peers, noderuntime.RaftEngineEtcd, migration)
		require.NoError(b, err)
		nodes = append(nodes, node)
	}
	waitForMultiGroupBootstrapClusterConfig(b, nodes, endpoints, groups, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	for want, gid := range groups {
		current := waitForGroupLeader(b, nodes, gid, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
		if current != want {
			admin, ok := nodes[current].engineForGroup(gid).(raftengine.Admin)
			require.True(b, ok)
			ctx, cancel := context.WithTimeout(b.Context(), bootstrapE2ERPCRequestTimeout)
			err := admin.TransferLeadershipToServer(ctx, endpoints[want].id, endpoints[want].raftAddrs[gid])
			cancel()
			require.NoError(b, err)
			waitForGroupLeaderOnNode(b, nodes, gid, want, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
		}
	}
	conn, err := grpc.NewClient(endpoints[0].raftAddrs[1], internalutil.GRPCDialOptions()...)
	require.NoError(b, err)
	defer func() { require.NoError(b, conn.Close()) }()
	client := pb.NewRawKVClient(conn)
	value := bytes.Repeat([]byte("v"), 1024)
	for i := range 128 {
		require.NoError(b, rawPutWithTimeout(client, fmt.Appendf(nil, "z-%04d", i), value))
	}
	readKey := []byte("a-read")
	require.NoError(b, rawPutWithTimeout(client, readKey, value))
	catalog := nodes[0].distCatalog
	snapshot, err := catalog.Snapshot(b.Context())
	require.NoError(b, err)
	require.Len(b, snapshot.Routes, 1)
	splitter := serverwiring.DistributionSplitter{Server: nodes[0].distServer}
	result, err := splitter.SplitRange(b.Context(), autosplit.SplitRequest{ExpectedCatalogVersion: snapshot.Version,
		RouteID: snapshot.Routes[0].RouteID, SplitKey: []byte("m"), TargetGroupID: 2})
	require.NoError(b, err)
	require.Eventually(b, func() bool {
		job, found, err := catalog.SplitJob(b.Context(), result.JobID)
		return err == nil && found && job.Phase == distribution.SplitJobPhaseFence && !job.TransferPreflightDone
	}, 3*time.Minute, 20*time.Millisecond)
	measurePreflightTraffic(b, client, catalog, result.JobID, readKey, value, samples)
}

func measurePreflightTraffic(b *testing.B, client pb.RawKVClient, catalog *distribution.CatalogStore, jobID uint64, readKey, value []byte, samples *preflightBenchSamples) {
	b.Helper()
	writeKey := []byte("a-write")
	start := time.Now()
	b.StartTimer()
	defer b.StopTimer()
	const interval = 200 * time.Millisecond
	scheduled := start
	for time.Since(start) < 2*time.Minute {
		job, found, err := catalog.SplitJob(b.Context(), jobID)
		require.NoError(b, err)
		require.True(b, found)
		if job.TransferPreflightDone {
			require.NotEmpty(b, job.TransferPreflightProgress)
			for _, progress := range job.TransferPreflightProgress {
				require.True(b, progress.Done)
			}
			b.ReportMetric(float64(len(job.TransferPreflightProgress)), "families/op")
			return
		}
		require.Equal(b, distribution.SplitJobPhaseFence, job.Phase)
		if wait := time.Until(scheduled); wait > 0 {
			time.Sleep(min(wait, 20*time.Millisecond))
			continue
		}
		// A slow RPC may span multiple deadlines. Record unsent slots and
		// keep the original cadence without replaying them in a burst.
		lag := time.Since(scheduled)
		samples.missed += int(lag / interval)
		samples.maxStartLag = max(samples.maxStartLag, lag)
		scheduled = scheduled.Add(lag.Truncate(interval) + interval)
		ctx, cancel := context.WithTimeout(b.Context(), bootstrapE2ERPCRequestTimeout)
		op := time.Now()
		put, err := client.RawPut(ctx, &pb.RawPutRequest{Key: writeKey, Value: value})
		samples.writes = append(samples.writes, time.Since(op))
		cancel()
		require.NoError(b, err)
		require.True(b, put.GetSuccess())
		op = time.Now()
		get, err := rawGetWithTimeout(client, readKey, bootstrapE2ERPCRequestTimeout)
		samples.reads = append(samples.reads, time.Since(op))
		require.NoError(b, err)
		require.True(b, get.GetExists())
		require.Equal(b, value, get.GetValue())
	}
	b.Fatal("timed out waiting for durable preflight completion")
}
