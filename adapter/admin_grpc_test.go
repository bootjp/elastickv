package adapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeGroup struct {
	leaderID     string
	leaderAddr   string
	term         uint64
	commit       uint64
	applied      uint64
	servers      []raftengine.Server
	cfgErr       error
	snapshotEach uint64
}

func (f fakeGroup) Status() raftengine.Status {
	return raftengine.Status{
		Leader:       raftengine.LeaderInfo{ID: f.leaderID, Address: f.leaderAddr},
		Term:         f.term,
		CommitIndex:  f.commit,
		AppliedIndex: f.applied,
	}
}

func (f fakeGroup) Configuration(context.Context) (raftengine.Configuration, error) {
	if f.cfgErr != nil {
		return raftengine.Configuration{}, f.cfgErr
	}
	return raftengine.Configuration{Servers: append([]raftengine.Server(nil), f.servers...)}, nil
}

func (f fakeGroup) SnapshotEvery() uint64 {
	if f.snapshotEach == 0 {
		return 10_000
	}
	return f.snapshotEach
}

func TestGetClusterOverviewReturnsSelfAndLeaders(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "node-a", GRPCAddress: "127.0.0.1:50051"},
		[]NodeIdentity{{NodeID: "node-b", GRPCAddress: "127.0.0.1:50052"}},
	)
	// Populate the live Raft config with both nodes so the bootstrap seed
	// for node-b is accepted by mergeSeedMembers (the scale-in fix only
	// drops seeds when the live config is authoritatively empty for that
	// NodeID — i.e., the node was removed via raft RemoveServer).
	servers := []raftengine.Server{
		{ID: "node-a", Address: "127.0.0.1:50051"},
		{ID: "node-b", Address: "127.0.0.1:50052"},
	}
	srv.RegisterGroup(1, fakeGroup{leaderID: "node-a", term: 7, servers: servers})
	srv.RegisterGroup(2, fakeGroup{leaderID: "node-b", term: 3, servers: servers})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatalf("GetClusterOverview: %v", err)
	}
	if resp.Self.NodeId != "node-a" {
		t.Fatalf("self = %q, want node-a", resp.Self.NodeId)
	}
	if len(resp.Members) != 1 || resp.Members[0].NodeId != "node-b" {
		t.Fatalf("members = %v, want [node-b]", resp.Members)
	}
	if len(resp.GroupLeaders) != 2 {
		t.Fatalf("group_leaders count = %d, want 2", len(resp.GroupLeaders))
	}
}

func TestGetNodeVersionReturnsConfiguredVersion(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1"},
		nil,
		WithAdminNodeVersion("v1.2.3"),
	)

	resp, err := srv.GetNodeVersion(context.Background(), &pb.GetNodeVersionRequest{})
	if err != nil {
		t.Fatalf("GetNodeVersion: %v", err)
	}
	if resp.NodeVersion != "v1.2.3" {
		t.Fatalf("NodeVersion = %q, want v1.2.3", resp.NodeVersion)
	}
}

func TestGetRaftGroupsExposesCommitApplied(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(NodeIdentity{NodeID: "n1"}, nil)
	srv.RegisterGroup(1, fakeGroupWithContact{leaderID: "n1", term: 2, commit: 99, applied: 97, lastContact: 5 * time.Second})

	// Freeze the per-server clock so the computed last-contact timestamp is
	// deterministic. No package-global state is mutated, so other parallel
	// tests cannot race through this seam.
	fixed := time.Unix(1_000_000, 0)
	srv.SetClock(func() time.Time { return fixed })

	resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups: %v", err)
	}
	if len(resp.Groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(resp.Groups))
	}
	g := resp.Groups[0]
	if g.CommitIndex != 99 || g.AppliedIndex != 97 || g.LeaderTerm != 2 {
		t.Fatalf("unexpected state %+v", g)
	}
	wantLastContact := fixed.Add(-5 * time.Second).UnixMilli()
	if g.LastContactUnixMs != wantLastContact {
		t.Fatalf("LastContactUnixMs = %d, want %d", g.LastContactUnixMs, wantLastContact)
	}
}

func TestGetRaftGroupsReportsLocalLeaderVersion(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		nil,
		WithAdminNodeVersion("v-local"),
	)
	srv.RegisterGroup(1, fakeGroup{leaderID: "n1", leaderAddr: "10.0.0.11:50051"})

	resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups: %v", err)
	}
	if got := resp.Groups[0].LeaderNodeVersion; got != "v-local" {
		t.Fatalf("LeaderNodeVersion = %q, want v-local", got)
	}
}

func TestGetRaftGroupsLeaderVersionAsync(t *testing.T) {
	t.Parallel()
	probeStarted := make(chan struct{}, 1)
	releaseProbe := make(chan struct{})
	var calls int
	var callsMu sync.Mutex
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		nil,
		WithAdminLeaderVersionProbeTimeout(2*time.Second),
		WithAdminLeaderVersionCacheTTL(time.Second),
		WithAdminLeaderVersionProbe(func(ctx context.Context, addr string) (string, error) {
			if addr != "10.0.0.12:50051" {
				t.Errorf("probe addr = %q, want 10.0.0.12:50051", addr)
			}
			callsMu.Lock()
			calls++
			callsMu.Unlock()
			probeStarted <- struct{}{}
			select {
			case <-releaseProbe:
				return "v-remote", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}),
	)
	srv.RegisterGroup(1, fakeGroup{leaderID: "n2", leaderAddr: "10.0.0.12:50051"})

	resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups first: %v", err)
	}
	requireLeaderVersion(t, resp, "", "first")
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("leader version probe was not started")
	}
	second, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups second: %v", err)
	}
	requireLeaderVersion(t, second, "", "second")
	close(releaseProbe)
	waitForLeaderVersion(t, srv, "v-remote")
	callsMu.Lock()
	gotCalls := calls
	callsMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("probe calls = %d, want 1 while cache is fresh", gotCalls)
	}
}

func TestGetRaftGroupsLeaderVersionProbeUsesServerClockForCacheStamp(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	var clockMu sync.Mutex
	now := base
	setNow := func(next time.Time) {
		clockMu.Lock()
		now = next
		clockMu.Unlock()
	}
	var callsMu sync.Mutex
	calls := 0
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		nil,
		WithAdminLeaderVersionProbeTimeout(time.Second),
		WithAdminLeaderVersionCacheTTL(time.Second),
		WithAdminLeaderVersionProbe(func(context.Context, string) (string, error) {
			callsMu.Lock()
			defer callsMu.Unlock()
			calls++
			if calls == 1 {
				return "v1", nil
			}
			return "v2", nil
		}),
	)
	srv.SetClock(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	})
	srv.RegisterGroup(1, fakeGroup{leaderID: "n2", leaderAddr: "10.0.0.12:50051"})

	_, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups first: %v", err)
	}
	waitForLeaderVersion(t, srv, "v1")

	setNow(base.Add(2 * time.Second))
	resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups after TTL: %v", err)
	}
	requireLeaderVersion(t, resp, "", "expired")
	waitForLeaderVersion(t, srv, "v2")
}

func TestGetRaftGroupsLeaderVersionStaleProbeCannotOverwriteFreshCache(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_000, 0)
	var clockMu sync.Mutex
	now := base
	setNow := func(next time.Time) {
		clockMu.Lock()
		now = next
		clockMu.Unlock()
	}
	probeCalls := make(chan chan leaderVersionProbeResult, 2)
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		nil,
		WithAdminLeaderVersionProbeTimeout(5*time.Second),
		WithAdminLeaderVersionCacheTTL(time.Second),
		WithAdminLeaderVersionProbe(blockingLeaderVersionProbe(probeCalls)),
	)
	srv.SetClock(func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	})
	srv.RegisterGroup(1, fakeGroup{leaderID: "n2", leaderAddr: "10.0.0.12:50051"})

	_, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	require.NoError(t, err)
	firstProbe := receiveLeaderVersionProbeCall(t, probeCalls, "first")

	setNow(base.Add(2 * time.Second))
	resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	require.NoError(t, err)
	requireLeaderVersion(t, resp, "", "second")
	secondProbe := receiveLeaderVersionProbeCall(t, probeCalls, "second")
	secondProbe <- leaderVersionProbeResult{version: "v-fresh"}
	waitForLeaderVersion(t, srv, "v-fresh")

	firstProbe <- leaderVersionProbeResult{version: "v-stale"}
	requireLeaderVersionNever(t, srv, "v-stale", 100*time.Millisecond)
	resp, err = srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	require.NoError(t, err)
	requireLeaderVersion(t, resp, "v-fresh", "after stale completion")
}

type leaderVersionProbeResult struct {
	version string
	err     error
}

func blockingLeaderVersionProbe(calls chan<- chan leaderVersionProbeResult) LeaderVersionProbe {
	return func(ctx context.Context, _ string) (string, error) {
		resultCh := make(chan leaderVersionProbeResult)
		select {
		case calls <- resultCh:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		select {
		case result := <-resultCh:
			return result.version, result.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func requireLeaderVersionNever(t *testing.T, srv *AdminServer, unwanted string, waitFor time.Duration) {
	t.Helper()
	require.Never(t, func() bool {
		resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
		return err == nil && len(resp.Groups) == 1 && resp.Groups[0].LeaderNodeVersion == unwanted
	}, waitFor, 10*time.Millisecond)
}

func receiveLeaderVersionProbeCall(
	t *testing.T,
	calls <-chan chan leaderVersionProbeResult,
	label string,
) chan leaderVersionProbeResult {
	t.Helper()
	select {
	case resultCh := <-calls:
		return resultCh
	case <-time.After(time.Second):
		t.Fatalf("%s leader version probe was not started", label)
		return nil
	}
}

func TestGetRaftGroupsLeaderVersionTriesAlternateAddressForSameLeaderID(t *testing.T) {
	t.Parallel()
	var callsMu sync.Mutex
	callsByAddr := map[string]int{}
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		nil,
		WithAdminLeaderVersionProbeTimeout(time.Second),
		WithAdminLeaderVersionCacheTTL(time.Second),
		WithAdminLeaderVersionProbe(func(_ context.Context, addr string) (string, error) {
			callsMu.Lock()
			callsByAddr[addr]++
			callsMu.Unlock()
			if addr == "10.0.0.12:50051" {
				return "", status.Error(codes.Unavailable, "stale listener")
			}
			return "v-good", nil
		}),
	)
	srv.RegisterGroup(1, fakeGroup{leaderID: "n2", leaderAddr: "10.0.0.12:50051"})
	srv.RegisterGroup(2, fakeGroup{leaderID: "n2", leaderAddr: "10.0.0.22:50051"})

	_, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatalf("GetRaftGroups first: %v", err)
	}
	require.Eventually(t, func() bool {
		resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
		if err != nil || len(resp.Groups) != 2 {
			return false
		}
		return resp.Groups[0].LeaderNodeVersion == "" && resp.Groups[1].LeaderNodeVersion == "v-good"
	}, time.Second, 10*time.Millisecond)
	callsMu.Lock()
	defer callsMu.Unlock()
	if callsByAddr["10.0.0.12:50051"] == 0 || callsByAddr["10.0.0.22:50051"] == 0 {
		t.Fatalf("probe calls by address = %v, want both stale and alternate addresses", callsByAddr)
	}
}

func requireLeaderVersion(t *testing.T, resp *pb.GetRaftGroupsResponse, want, label string) {
	t.Helper()
	if len(resp.Groups) != 1 {
		t.Fatalf("%s groups = %d, want 1", label, len(resp.Groups))
	}
	if got := resp.Groups[0].LeaderNodeVersion; got != want {
		t.Fatalf("%s LeaderNodeVersion = %q, want %q", label, got, want)
	}
}

func waitForLeaderVersion(t *testing.T, srv *AdminServer, want string) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
		if err != nil {
			t.Fatalf("GetRaftGroups while waiting for version: %v", err)
		}
		if len(resp.Groups) == 1 && resp.Groups[0].LeaderNodeVersion == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("leader version cache was not populated with %q", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestGetRaftGroupsLeaderVersionProbePropagatesAuthMetadata(t *testing.T) {
	t.Parallel()
	gotAuth := make(chan []string, 1)
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		nil,
		WithAdminLeaderVersionProbeTimeout(time.Second),
		WithAdminLeaderVersionProbe(func(ctx context.Context, _ string) (string, error) {
			md, _ := metadata.FromOutgoingContext(ctx)
			gotAuth <- md.Get("authorization")
			return "v-remote", nil
		}),
	)
	srv.RegisterGroup(1, fakeGroup{leaderID: "n2", leaderAddr: "10.0.0.12:50051"})

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer admin-token"),
	)
	if _, err := srv.GetRaftGroups(ctx, &pb.GetRaftGroupsRequest{}); err != nil {
		t.Fatalf("GetRaftGroups: %v", err)
	}
	select {
	case got := <-gotAuth:
		if len(got) != 1 || got[0] != "Bearer admin-token" {
			t.Fatalf("authorization metadata = %v, want bearer token", got)
		}
	case <-time.After(time.Second):
		t.Fatal("leader version probe did not run")
	}
}

// TestGetClusterOverviewUnionsSeedsAndLiveConfig asserts that
// GetClusterOverview picks up a node that was added to a Raft group after the
// admin server was constructed (scale-out). Without Configuration polling,
// the static seed list would miss it entirely.
func TestGetClusterOverviewUnionsSeedsAndLiveConfig(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		[]NodeIdentity{{NodeID: "n2", GRPCAddress: "10.0.0.12:50051"}},
	)
	// Group reports a member (n3) that is NOT in the bootstrap seed list.
	srv.RegisterGroup(1, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: "10.0.0.12:50051"},
			{ID: "n3", Address: "10.0.0.13:50051"},
		},
	})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]string)
	for _, m := range resp.Members {
		ids[m.NodeId] = m.GrpcAddress
	}
	// Self (n1) is excluded; both seed (n2) and live-config (n3) must appear.
	if len(ids) != 2 {
		t.Fatalf("members = %v, want {n2, n3}", ids)
	}
	if ids["n2"] != "10.0.0.12:50051" || ids["n3"] != "10.0.0.13:50051" {
		t.Fatalf("unexpected members %v", ids)
	}
}

// TestGetClusterOverviewDuplicateMemberIDsDeterministic pins the tie-break
// when two Raft groups disagree on a server's address (e.g. mid-readdress,
// before every group has converged): the group with the smallest ID wins and
// the result is stable across calls, so fan-out doesn't flap between stale
// and current addresses.
func TestGetClusterOverviewDuplicateMemberIDsDeterministic(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(NodeIdentity{NodeID: "n1"}, nil)
	// Group 1 (lower ID): n2 already moved to new address.
	srv.RegisterGroup(1, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: "10.0.0.22:50051"},
		},
	})
	// Group 7 (higher ID): still reports n2 at the stale address.
	srv.RegisterGroup(7, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: "10.0.0.12:50051"},
		},
	})

	// Run overview 5 times. All must return the low-ID group's n2 address.
	for i := 0; i < 5; i++ {
		resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Members) != 1 {
			t.Fatalf("iter %d: members=%d want 1", i, len(resp.Members))
		}
		if resp.Members[0].GrpcAddress != "10.0.0.22:50051" {
			t.Fatalf("iter %d: got %s, want low-ID group's n2 @ 10.0.0.22:50051", i, resp.Members[0].GrpcAddress)
		}
	}
}

// TestGetClusterOverviewSeedBackfillsBlankLiveAddress asserts that when a
// Raft group reports a server with NodeID set but Address="" (the etcd
// engine emits these mid-membership-update), the seed list still gets to
// backfill that ID instead of being shadowed by a blank live entry. Without
// this, GetClusterOverview would drop the peer from fan-out entirely until
// the live Configuration converged.
func TestGetClusterOverviewSeedBackfillsBlankLiveAddress(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1"},
		[]NodeIdentity{{NodeID: "n2", GRPCAddress: "10.0.0.12:50051"}},
	)
	// Live config knows n2 exists but has no address yet.
	srv.RegisterGroup(1, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: ""},
		},
	})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Members) != 1 {
		t.Fatalf("members = %d, want 1", len(resp.Members))
	}
	got := resp.Members[0]
	if got.NodeId != "n2" || got.GrpcAddress != "10.0.0.12:50051" {
		t.Fatalf("members[0] = %+v, want seed n2 @ 10.0.0.12:50051 (blank live skipped)", got)
	}
}

// TestGetClusterOverviewDropsRemovedSeedAfterScaleIn asserts that a node
// that was removed from the live Raft configuration is also dropped from
// GetClusterOverview, even when it remains in the bootstrap seed list.
// Codex P2 on 14698e8d: previously mergeSeedMembers re-added any seed whose
// NodeID was missing from live config, so a decommissioned peer stayed in
// the overview forever and the admin fan-out kept dialing it.
func TestGetClusterOverviewDropsRemovedSeedAfterScaleIn(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1"},
		// Bootstrap remembered both n2 and n3, but n3 has since been removed.
		[]NodeIdentity{
			{NodeID: "n2", GRPCAddress: "10.0.0.12:50051"},
			{NodeID: "n3", GRPCAddress: "10.0.0.13:50051"},
		},
	)
	srv.RegisterGroup(1, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: "10.0.0.12:50051"},
			// n3 absent — was removed via raft RemoveServer.
		},
	})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Members) != 1 || resp.Members[0].NodeId != "n2" {
		t.Fatalf("members = %v, want [n2] only (n3 removed via scale-in)", resp.Members)
	}
}

// TestFanoutConfigurationCallsBoundedConcurrency asserts that
// fanoutConfigurationCalls never has more than configFanoutMaxConcurrency
// Configuration goroutines inside the RPC call at once, even when the node
// hosts more groups than the cap. Prevents a goroutine/conn burst on every
// GetClusterOverview when a node carries hundreds of shards.
func TestFanoutConfigurationCallsBoundedConcurrency(t *testing.T) {
	t.Parallel()
	const numGroups = configFanoutMaxConcurrency * 4

	var (
		mu       sync.Mutex
		inFlight int
		peakSeen int
		release  = make(chan struct{})
	)
	groups := make([]groupEntry, 0, numGroups)
	for i := 0; i < numGroups; i++ {

		groups = append(groups, groupEntry{
			id: uint64(i + 1), //nolint:gosec // i ranges over a small bounded loop; conversion is safe.
			group: testProbeGroup{cb: func() {
				mu.Lock()
				inFlight++
				if inFlight > peakSeen {
					peakSeen = inFlight
				}
				mu.Unlock()
				<-release // hold the call open until released
				mu.Lock()
				inFlight--
				mu.Unlock()
			}},
		})
	}

	done := make(chan struct{})
	go func() {
		_ = fanoutConfigurationCalls(t.Context(), groups)
		close(done)
	}()

	// Give the runtime a beat to schedule goroutines and pump them through
	// the semaphore. We don't need a precise wait — peakSeen is a high-water
	// mark, so any sampling moment works as long as the calls have stalled.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		if inFlight >= configFanoutMaxConcurrency {
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	got := peakSeen
	mu.Unlock()
	if got > configFanoutMaxConcurrency {
		t.Fatalf("peak in-flight = %d, want ≤%d", got, configFanoutMaxConcurrency)
	}

	close(release) // let everything drain
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fanoutConfigurationCalls did not return after release")
	}
}

// testProbeGroup is a minimal AdminGroup for concurrency tests; it runs cb
// on every Configuration call so the test can observe in-flight count.
type testProbeGroup struct {
	cb func()
}

func (t testProbeGroup) Status() raftengine.Status { return raftengine.Status{} }
func (t testProbeGroup) Configuration(context.Context) (raftengine.Configuration, error) {
	if t.cb != nil {
		t.cb()
	}
	return raftengine.Configuration{}, nil
}
func (t testProbeGroup) SnapshotEvery() uint64 { return 10_000 }

// TestGetClusterOverviewKeepsSeedOnPartialConfigFailure asserts that a
// bootstrap seed is NOT pruned when one group's Configuration succeeds and
// another errors — a node visible only in the failing group would otherwise
// disappear from fan-out for transient reasons. Codex flagged this on
// 94851380: the round-24 fix flipped authoritative on any single success,
// which incorrectly pruned peers under partial failure. The current contract
// requires every queried group to succeed before pruning.
func TestGetClusterOverviewKeepsSeedOnPartialConfigFailure(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1"},
		[]NodeIdentity{
			{NodeID: "n2", GRPCAddress: "10.0.0.12:50051"},
			// n3 is only known via the seed list; the failing group below
			// is the one that would have reported it.
			{NodeID: "n3", GRPCAddress: "10.0.0.13:50051"},
		},
	)
	srv.RegisterGroup(1, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: "10.0.0.12:50051"},
			// n3 absent from this group.
		},
	})
	// Group 7 errors — n3 happens to live in this group's config.
	srv.RegisterGroup(7, fakeGroup{leaderID: "n1", cfgErr: context.DeadlineExceeded})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, m := range resp.Members {
		ids[m.NodeId] = true
	}
	if !ids["n2"] {
		t.Fatalf("n2 missing from members %v — live group reported it", resp.Members)
	}
	if !ids["n3"] {
		t.Fatalf("n3 dropped under partial failure: members %v — seeds must fall through when any group errors", resp.Members)
	}
}

// TestGetClusterOverviewLiveConfigWinsOverStaleSeed asserts that when a node
// is readdressed (same NodeID, new GRPCAddress), the live Raft Configuration
// wins over the stale bootstrap seed so fan-out dials the current endpoint.
// Codex P2 on e1f0e532: previously seed was added first and later entries
// with the same ID were ignored, silently pinning the old address.
func TestGetClusterOverviewLiveConfigWinsOverStaleSeed(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1", GRPCAddress: "10.0.0.11:50051"},
		// Bootstrap: n2 lived at 10.0.0.12.
		[]NodeIdentity{{NodeID: "n2", GRPCAddress: "10.0.0.12:50051"}},
	)
	// Raft config reports n2 moved to 10.0.0.22.
	srv.RegisterGroup(1, fakeGroup{
		leaderID: "n1", term: 1,
		servers: []raftengine.Server{
			{ID: "n1", Address: "10.0.0.11:50051"},
			{ID: "n2", Address: "10.0.0.22:50051"},
		},
	})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Members) != 1 {
		t.Fatalf("members = %d, want 1", len(resp.Members))
	}
	got := resp.Members[0]
	if got.NodeId != "n2" || got.GrpcAddress != "10.0.0.22:50051" {
		t.Fatalf("members[0] = %+v, want n2 @ 10.0.0.22:50051 (live wins over seed)", got)
	}
}

// TestGetClusterOverviewSurvivesConfigurationError asserts that a group that
// errors on Configuration() does NOT fail the RPC — seed members are still
// returned.
func TestGetClusterOverviewSurvivesConfigurationError(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "n1"},
		[]NodeIdentity{{NodeID: "n2", GRPCAddress: "10.0.0.12:50051"}},
	)
	srv.RegisterGroup(1, fakeGroup{leaderID: "n1", cfgErr: context.DeadlineExceeded})

	resp, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatalf("overview should not fail on group config error: %v", err)
	}
	if len(resp.Members) != 1 || resp.Members[0].NodeId != "n2" {
		t.Fatalf("unexpected members %v", resp.Members)
	}
}

// TestGetRaftGroupsMapsUnknownLastContactToZero pins the sentinel-negative
// handling for raftengine's "unknown last contact" value (-1). The RPC
// reports 0 (epoch) in that case so the UI renders "unknown" rather than
// "contacted just now".
func TestGetRaftGroupsMapsUnknownLastContactToZero(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(NodeIdentity{NodeID: "n1"}, nil)
	srv.RegisterGroup(1, fakeGroupWithContact{leaderID: "n1", term: 1, lastContact: -1})

	fixed := time.Unix(2_000_000, 0)
	srv.SetClock(func() time.Time { return fixed })

	resp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Groups[0].LastContactUnixMs; got != 0 {
		t.Fatalf("LastContactUnixMs = %d, want 0 (unknown sentinel)", got)
	}
}

type fakeGroupWithContact struct {
	leaderID    string
	term        uint64
	commit      uint64
	applied     uint64
	lastContact time.Duration
}

func (f fakeGroupWithContact) Status() raftengine.Status {
	return raftengine.Status{
		Leader:       raftengine.LeaderInfo{ID: f.leaderID},
		Term:         f.term,
		CommitIndex:  f.commit,
		AppliedIndex: f.applied,
		LastContact:  f.lastContact,
	}
}

func (f fakeGroupWithContact) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (f fakeGroupWithContact) SnapshotEvery() uint64 { return 10_000 }

// TestGroupOrderingIsStable locks in deterministic ascending-by-RaftGroupId
// ordering so admin UIs and diff-based tests do not see rows jump around.
func TestGroupOrderingIsStable(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(NodeIdentity{NodeID: "n1"}, nil)
	for _, id := range []uint64{7, 2, 5, 3, 1} {
		srv.RegisterGroup(id, fakeGroup{leaderID: "n1"})
	}

	groupsResp, err := srv.GetRaftGroups(context.Background(), &pb.GetRaftGroupsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	gotGroups := make([]uint64, 0, len(groupsResp.Groups))
	for _, g := range groupsResp.Groups {
		gotGroups = append(gotGroups, g.RaftGroupId)
	}
	wantGroups := []uint64{1, 2, 3, 5, 7}
	if !equalU64s(gotGroups, wantGroups) {
		t.Fatalf("GetRaftGroups order = %v, want %v", gotGroups, wantGroups)
	}

	overview, err := srv.GetClusterOverview(context.Background(), &pb.GetClusterOverviewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	gotLeaders := make([]uint64, 0, len(overview.GroupLeaders))
	for _, gl := range overview.GroupLeaders {
		gotLeaders = append(gotLeaders, gl.RaftGroupId)
	}
	if !equalU64s(gotLeaders, wantGroups) {
		t.Fatalf("GetClusterOverview leader order = %v, want %v", gotLeaders, wantGroups)
	}
}

func equalU64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestAdminTokenAuth(t *testing.T) {
	t.Parallel()
	unary, _ := AdminTokenAuth("s3cret")
	if unary == nil {
		t.Fatal("interceptor should be non-nil for configured token")
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/" + pb.Admin_ServiceDesc.ServiceName + "/GetClusterOverview"}
	handler := func(_ context.Context, _ any) (any, error) { return "ok", nil }

	cases := []struct {
		name string
		md   metadata.MD
		code codes.Code
		call bool
	}{
		{"missing metadata", nil, codes.Unauthenticated, false},
		{"missing header", metadata.Pairs(), codes.Unauthenticated, false},
		{"wrong scheme", metadata.Pairs("authorization", "Basic zzz"), codes.Unauthenticated, false},
		{"wrong token", metadata.Pairs("authorization", "Bearer nope"), codes.Unauthenticated, false},
		{"correct", metadata.Pairs("authorization", "Bearer s3cret"), codes.OK, true},
	}
	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.md != nil {
				ctx = metadata.NewIncomingContext(ctx, tc.md)
			}
			resp, err := unary(ctx, nil, info, handler)
			if tc.code == codes.OK {
				if err != nil {
					t.Fatalf("want OK, got %v", err)
				}
				if resp != "ok" {
					t.Fatalf("handler not called: resp=%v", resp)
				}
				return
			}
			if status.Code(err) != tc.code {
				t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tc.code, err)
			}
		})
	}
}

func TestAdminTokenAuthSkipsOtherServices(t *testing.T) {
	t.Parallel()
	unary, _ := AdminTokenAuth("s3cret")
	info := &grpc.UnaryServerInfo{FullMethod: "/RawKV/Get"}
	handler := func(_ context.Context, _ any) (any, error) { return "ok", nil }

	resp, err := unary(context.Background(), nil, info, handler)
	if err != nil {
		t.Fatalf("non-admin method should not be gated: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("handler not called: resp=%v", resp)
	}
}

func TestAdminTokenAuthEmptyTokenDisabled(t *testing.T) {
	t.Parallel()
	unary, stream := AdminTokenAuth("")
	if unary != nil || stream != nil {
		t.Fatal("empty token should disable interceptors")
	}
}

// hangingGroup never returns from Configuration until ctx fires. Used to
// prove collectLiveMembers stops blocking the merge phase as soon as the
// caller cancels, even if one Configuration call is stuck.
type hangingGroup struct{ fakeGroup }

func (h hangingGroup) Configuration(ctx context.Context) (raftengine.Configuration, error) {
	<-ctx.Done()
	return raftengine.Configuration{}, ctx.Err()
}

// TestCollectLiveMembersHonoursCtxCancel asserts that collectLiveMembers
// returns promptly when ctx is cancelled, even if one Configuration call
// is stuck. Pre-fix, the wg.Wait() inside collectLiveMembers would block
// the merge phase (and the entire GetClusterOverview RPC) on the slowest
// group regardless of ctx state. Post-fix, the merge runs over whatever
// landed before the cancel; the stuck Configuration goroutine unwinds
// asynchronously when its ctx.Done fires.
func TestCollectLiveMembersHonoursCtxCancel(t *testing.T) {
	t.Parallel()

	groups := []groupEntry{
		{id: 1, group: hangingGroup{}},
		{id: 2, group: hangingGroup{}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	resCh := make(chan liveMembers, 1)
	go func() {
		resCh <- collectLiveMembers(ctx, groups, "self")
	}()

	select {
	case r := <-resCh:
		// With ctx already cancelled, no live config landed; expect empty maps.
		if len(r.addrByID) != 0 || len(r.order) != 0 {
			t.Fatalf("expected empty results on early cancel, got addrByID=%v order=%v", r.addrByID, r.order)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("collectLiveMembers blocked past 1s despite cancelled ctx — wg.Wait() regression?")
	}
}
