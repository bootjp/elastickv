package adapter

import (
	"context"
	"testing"

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
}

func (f fakeGroup) Status() raftengine.Status {
	return raftengine.Status{
		Leader:       raftengine.LeaderInfo{ID: f.leaderID},
		Term:         f.term,
		CommitIndex:  f.commit,
		AppliedIndex: f.applied,
	}
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
	srv.RegisterGroup(1, fakeGroup{leaderID: "n1", term: 2, commit: 99, applied: 97})

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
