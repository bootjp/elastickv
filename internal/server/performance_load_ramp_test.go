package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/loadtest"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/serverwiring"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rampCluster struct {
	nodes       []*bootstrapE2ENode
	endpoints   []bootstrapE2EMultiGroupEndpoint
	certificate string
}

func newRampCluster(t *testing.T) rampCluster {
	t.Helper()
	flags := enableMigrationRuntimeFlags(t)
	cert, key := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, key))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	token := "local-load-ramp-token"
	internalutil.ConfigureGRPCForwardToken(token)
	t.Cleanup(func() { internalutil.ConfigureGRPCForwardToken("") })
	flags.SplitMigrationTokenFile = filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(flags.SplitMigrationTokenFile, []byte(token), 0600))
	_, auth, err := configureAdminService(flags, false, adapter.NodeIdentity{}, nil, nil)
	require.NoError(t, err)
	groups := []uint64{1, 2}
	endpoints, listeners := allocateBootstrapE2EMultiGroupEndpoints(t, 3, groups)
	cluster := rampCluster{endpoints: endpoints, certificate: cert}
	t.Cleanup(func() { closeBootstrapE2ENodes(t, cluster.nodes) })
	peers := bootstrapGroupPeersArg(endpoints, groups)
	for i := range endpoints {
		node, err := startBootstrapE2EMultiGroupNode(t.TempDir(), endpoints[i], listeners[i], true, groups, peers,
			noderuntime.RaftEngineEtcd, bootstrapE2EMigrationConfig{flags: flags, auth: auth, token: token})
		require.NoError(t, err)
		cluster.nodes = append(cluster.nodes, node)
	}
	waitForMultiGroupBootstrapClusterConfig(t, cluster.nodes, endpoints, groups, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	pinPerformanceSplitLeaders(t, cluster.nodes, endpoints)
	return cluster
}

type rampAudit struct {
	Type         string          `json:"type"`
	Acknowledged int             `json:"acknowledged_writes"`
	Expected     int             `json:"expected_observations"`
	Audit        loadtest.Metric `json:"audit"`
	Verified     bool            `json:"verified"`
}

type rampStageStart struct {
	Index int
	Rate  int
	Time  time.Time
}

type rampProcess struct {
	stageStarts chan rampStageStart
	ready       chan struct{}
	done        chan error
	exited      chan struct{}
	stderr      *bytes.Buffer
	stages      []loadtest.Stage
	audit       rampAudit
}

func startRampLoad(t *testing.T, cluster rampCluster, profile *rampProfiler, auditReady string) *rampProcess {
	t.Helper()
	binary := os.Getenv("ELASTICKV_LOAD_BINARY")
	require.NotEmpty(t, binary, "build cmd/loadtest for this Linux host")
	var addresses []string
	for _, endpoint := range cluster.endpoints {
		addresses = append(addresses, endpoint.raftAddrs[1])
	}
	rates := rampEnv("ELASTICKV_LOAD_RATES", "100,500,1000,2000")
	args := []string{"--addresses", strings.Join(addresses, ","), "--ca-file", cluster.certificate,
		"--rates", rates,
		"--stage-duration", rampEnv("ELASTICKV_LOAD_STAGE_DURATION", "90s"),
		"--concurrency", rampEnv("ELASTICKV_LOAD_CONCURRENCY", "32"),
		"--read-percent", rampEnv("ELASTICKV_LOAD_READ_PERCENT", "50"),
		"--audit-ready-file", auditReady, "--prefix", "split-ramp", "--max-requests", rampEnv("ELASTICKV_LOAD_MAX_REQUESTS", "1000000"), "--verify-writes=true"}
	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, binary, args...)
	var stderr bytes.Buffer
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	result := &rampProcess{ready: make(chan struct{}), stageStarts: make(chan rampStageStart, len(strings.Split(rates, ","))), done: make(chan error, 1), exited: make(chan struct{}), stderr: &stderr}
	t.Cleanup(func() { cancel(); <-result.exited })
	go func() {
		defer close(result.exited)
		scanErr := readRampOutput(t, stdout, result, profile, len(strings.Split(rates, ",")))
		err := cmd.Wait()
		if scanErr != nil {
			err = fmt.Errorf("read load output: %w", scanErr)
		}
		result.done <- err
	}()
	select {
	case <-result.ready:
	case err := <-result.done:
		t.Fatalf("load ended before seed: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("load seed timed out")
	}
	return result
}

func rampEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// TestLinuxSplitLoadRamp is opt-in. Nodes share a process and disk, while the
// load generator is a separate process so its CPU is excluded from profiles.
func TestLinuxSplitLoadRamp(t *testing.T) {
	if os.Getenv("ELASTICKV_LOAD_RAMP") != "1" {
		t.Skip("set ELASTICKV_LOAD_RAMP=1 and ELASTICKV_LOAD_BINARY")
	}
	cluster := newRampCluster(t)
	profile := newRampProfiler(t)
	auditReady := filepath.Join(t.TempDir(), "audit-ready")
	process := startRampLoad(t, cluster, profile, auditReady)
	mode := rampEnv("ELASTICKV_LOAD_SPLIT", "cross")
	if mode != "none" {
		runRampSplit(t, cluster, mode, profile, process)
	}
	splitDone := time.Now()
	require.NoError(t, os.WriteFile(auditReady, []byte("catalog converged\n"), 0600))
	loadErr := <-process.done
	t.Logf("LOAD process_exit=%v", loadErr)
	if loadErr != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, loadErr, &exit)
		require.Equal(t, 1, exit.ExitCode())
		require.Contains(t, process.stderr.String(), "load degraded:")
	}
	require.Len(t, process.stages, len(strings.Split(rampEnv("ELASTICKV_LOAD_RATES", "100,500,1000,2000"), ",")))
	require.Equal(t, "audit", process.audit.Type, "a crash or early exit is not a passing run")
	acknowledged := 0
	for _, stage := range process.stages {
		acknowledged += stage.Write.Success
	}
	require.Equal(t, acknowledged, process.audit.Acknowledged)
	require.Positive(t, process.audit.Acknowledged)
	if mode != "none" {
		last := process.stages[len(process.stages)-1]
		duration, err := time.ParseDuration(rampEnv("ELASTICKV_LOAD_STAGE_DURATION", "90s"))
		require.NoError(t, err)
		loadEnd := last.Started.Add(duration)
		require.False(t, splitDone.After(loadEnd), "load stopped before Split completed; increase the stage duration")
	}
	require.Equal(t, process.audit.Acknowledged*len(cluster.endpoints), process.audit.Expected)
	require.Equal(t, process.audit.Expected, process.audit.Audit.Attempts)
	require.Equal(t, process.audit.Expected, process.audit.Audit.Success)
	require.True(t, process.audit.Verified)
	for _, stage := range process.stages {
		require.False(t, stage.Cancelled)
		require.Equal(t, stage.Planned, stage.Offered)
		require.Equal(t, stage.Offered, stage.Admitted+stage.DroppedCapacity+stage.DroppedLate)
		require.Zero(t, stage.Read.Errors["value_mismatch"])
		require.Zero(t, stage.Verification.Errors["value_mismatch"])
	}
	if mode == "cross" {
		verifySplitWriteRecovery(t, cluster.nodes, cluster.endpoints)
	}
	profile.wait()
}

func runRampSplit(t *testing.T, cluster rampCluster, mode string, profile *rampProfiler, process *rampProcess) {
	t.Helper()
	require.Contains(t, []string{"same", "cross"}, mode)
	target := 0
	if len(strings.Split(rampEnv("ELASTICKV_LOAD_RATES", "100,500,1000,2000"), ",")) > 1 {
		target = 1
	}
	if text := os.Getenv("ELASTICKV_LOAD_SPLIT_STAGE"); text != "" {
		value, err := strconv.Atoi(text)
		require.NoError(t, err)
		target = value
	}
	require.GreaterOrEqual(t, target, 0)
	require.Less(t, target, len(strings.Split(rampEnv("ELASTICKV_LOAD_RATES", "100,500,1000,2000"), ",")))
	var event rampStageStart
	for {
		select {
		case event = <-process.stageStarts:
			if event.Index >= target {
				goto ready
			}
		case <-process.exited:
			t.Fatal("load completed before requested Split stage")
		case <-time.After(time.Minute * 6):
			t.Fatal("requested Split stage did not start")
		}
	}
ready:
	t.Logf("LOAD split_start stage=%d offered_rps=%d stage_started=%s", event.Index, event.Rate, event.Time.UTC().Format(time.RFC3339Nano))
	source := waitForGroupLeader(t, cluster.nodes, 1, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	catalog := cluster.nodes[source].distCatalog
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	splitter := serverwiring.DistributionSplitter{Server: cluster.nodes[source].distServer}
	same := requestRampSplit(t, splitter, catalog, autosplit.SplitRequest{
		RouteID: snapshot.Routes[0].RouteID, SplitKey: []byte("m")})
	waitForCatalogVersion(t, cluster.nodes, same.CatalogVersion, bootstrapE2EWaitTimeout, bootstrapE2EWaitInterval)
	t.Logf("LOAD split_same_done version=%d", same.CatalogVersion)
	if mode == "same" {
		return
	}
	time.Sleep(5 * time.Second)
	started := time.Now()
	result := requestRampSplit(t, splitter, catalog, autosplit.SplitRequest{
		RouteID: same.Right.RouteID, SplitKey: []byte("t"), TargetGroupID: 2})
	watch := watchPerformancePreflight(t, catalog, result.JobID)
	stop := profile.watchJob(catalog, result.JobID)
	done, phase, lastError := waitPerformanceSplitJob(t, catalog, result.JobID, started)
	stop()
	watch()
	require.True(t, done, "phase=%d error=%s", phase, lastError)
	require.Equal(t, distribution.SplitJobPhaseDone, phase)
	t.Logf("LOAD split_cross_done job=%s elapsed_ms=%d", strconv.FormatUint(result.JobID, 10), time.Since(started).Milliseconds())
	finalJob, found, err := catalog.SplitJob(t.Context(), result.JobID)
	require.NoError(t, err)
	require.True(t, found)
	t.Logf("LOAD online_capture_version=%d sequence=%d seal_sequence=%d", finalJob.OnlineCaptureVersion, finalJob.OnlineSequence, finalJob.OnlineSealSequence)
	if os.Getenv("ELASTICKV_LOAD_REQUIRE_ONLINE") == "1" {
		require.Equal(t, uint32(1), finalJob.OnlineCaptureVersion)
		require.Positive(t, finalJob.OnlineSequence)
		require.Equal(t, finalJob.OnlineSequence, finalJob.OnlineSealSequence)
	}

	waitForSplitRecoveryRoutes(t, cluster.nodes, source)
}

func readRampOutput(t *testing.T, reader io.Reader, result *rampProcess, profile *rampProfiler, stageCount int) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		t.Logf("LOAD %s", line)
		var event struct {
			Type  string         `json:"type"`
			Index int            `json:"index"`
			Rate  int            `json:"rate"`
			Time  time.Time      `json:"time"`
			Stage loadtest.Stage `json:"stage"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			t.Errorf("load output: %v", err)
			continue
		}
		switch event.Type {
		case "seeded":
			close(result.ready)
		case "stage_start":
			result.stageStarts <- rampStageStart{Index: event.Index, Rate: event.Rate, Time: event.Time}
			if event.Index == stageCount-1 {
				profile.capture("peak")
			}
		case "stage":
			result.stages = append(result.stages, event.Stage)
		case "audit":
			if err := json.Unmarshal(line, &result.audit); err != nil {
				t.Errorf("audit: %v", err)
			}
		}
	}
	return scanner.Err()
}

// A definitive OCC rejection has not applied the Split. Refresh only that
// precondition; never replay an ambiguous control-plane mutation.
func requestRampSplit(t *testing.T, splitter serverwiring.DistributionSplitter, catalog *distribution.CatalogStore, request autosplit.SplitRequest) autosplit.SplitResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		require.NoError(t, ctx.Err())
		snapshot, err := catalog.Snapshot(ctx)
		require.NoError(t, err)
		request.ExpectedCatalogVersion = snapshot.Version
		require.NoError(t, ctx.Err())
		result, err := splitter.SplitRange(ctx, request)
		if err == nil {
			return result
		}
		require.True(t, rampCatalogConflict(err), "Split outcome is not a definitive OCC rejection: %v", err)
		t.Logf("LOAD split_occ_retry version=%d", snapshot.Version)
		select {
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func rampCatalogConflict(err error) bool {
	var rpc interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &rpc) {
		return false
	}
	detail := rpc.GRPCStatus()
	return detail != nil && detail.Code() == codes.Aborted && detail.Message() == "catalog version conflict"
}
