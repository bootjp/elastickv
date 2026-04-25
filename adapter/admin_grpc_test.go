package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeGroup struct {
	leaderID string
	term     uint64
	commit   uint64
	applied  uint64
	servers  []raftengine.Server
	cfgErr   error
}

func (f fakeGroup) Status() raftengine.Status {
	return raftengine.Status{
		Leader:       raftengine.LeaderInfo{ID: f.leaderID},
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

func TestGetClusterOverviewReturnsSelfAndLeaders(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(
		NodeIdentity{NodeID: "node-a", GRPCAddress: "127.0.0.1:50051"},
		[]NodeIdentity{{NodeID: "node-b", GRPCAddress: "127.0.0.1:50052"}},
	)
	srv.RegisterGroup(1, fakeGroup{leaderID: "node-a", term: 7})
	srv.RegisterGroup(2, fakeGroup{leaderID: "node-b", term: 3})

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
