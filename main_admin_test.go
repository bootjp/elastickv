package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
)

func TestConfigureAdminServiceDisabledByDefault(t *testing.T) {
	t.Parallel()
	srv, icept, err := configureAdminService("", false, adapter.NodeIdentity{NodeID: "n1"}, nil)
	if err != nil {
		t.Fatalf("disabled-by-default should not error: %v", err)
	}
	if srv != nil || !icept.empty() {
		t.Fatalf("disabled service should return nil server and empty interceptors; got %v %+v", srv, icept)
	}
}

func TestConfigureAdminServiceRejectsMutualExclusion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokPath := filepath.Join(dir, "t")
	if err := os.WriteFile(tokPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := configureAdminService(tokPath, true, adapter.NodeIdentity{}, nil); err == nil {
		t.Fatal("expected mutual-exclusion error")
	}
}

func TestConfigureAdminServiceTokenFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokPath := filepath.Join(dir, "t")
	if err := os.WriteFile(tokPath, []byte("hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, icept, err := configureAdminService(tokPath, false, adapter.NodeIdentity{NodeID: "n1"}, nil)
	if err != nil {
		t.Fatalf("configureAdminService: %v", err)
	}
	if srv == nil {
		t.Fatal("expected an AdminServer instance")
	}
	// Expect one unary + one stream interceptor for the admin-token gate.
	if len(icept.unary) != 1 || len(icept.stream) != 1 {
		t.Fatalf("expected 1 unary + 1 stream interceptor, got %d + %d", len(icept.unary), len(icept.stream))
	}
}

func TestConfigureAdminServiceInsecureNoAuth(t *testing.T) {
	t.Parallel()
	srv, icept, err := configureAdminService("", true, adapter.NodeIdentity{NodeID: "n1"}, nil)
	if err != nil {
		t.Fatalf("insecure mode should succeed: %v", err)
	}
	if srv == nil {
		t.Fatal("expected AdminServer in insecure mode")
	}
	if !icept.empty() {
		t.Fatalf("insecure mode should not attach interceptors, got %+v", icept)
	}
}

func TestAdminMembersFromBootstrapExcludesSelf(t *testing.T) {
	t.Parallel()
	servers := []raftengine.Server{
		{ID: "n1", Address: "10.0.0.11:50051"},
		{ID: "n2", Address: "10.0.0.12:50051"},
		{ID: "n3", Address: "10.0.0.13:50051"},
	}
	got := adminMembersFromBootstrap("n1", servers)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (self excluded)", len(got))
	}
	want := map[string]string{"n2": "10.0.0.12:50051", "n3": "10.0.0.13:50051"}
	for _, m := range got {
		if want[m.NodeID] != m.GRPCAddress {
			t.Fatalf("member %+v not in expected set %v", m, want)
		}
	}
}

func TestAdminMembersFromBootstrapEmpty(t *testing.T) {
	t.Parallel()
	if got := adminMembersFromBootstrap("n1", nil); got != nil {
		t.Fatalf("empty bootstrap should produce nil, got %v", got)
	}
	single := []raftengine.Server{{ID: "n1", Address: "a:1"}}
	if got := adminMembersFromBootstrap("n1", single); len(got) != 0 {
		t.Fatalf("single-node bootstrap should yield no members, got %v", got)
	}
}

// TestCanonicalSelfAddressPicksLowestGroup pins the deterministic choice of
// Self.GRPCAddress when --raftGroups is set — the fan-out path has to dial an
// endpoint that this process actually listens on, so --address (which may be
// unrelated) must not win over the real group listeners.
func TestCanonicalSelfAddressPicksLowestGroup(t *testing.T) {
	t.Parallel()
	runtimes := []*raftGroupRuntime{
		{spec: groupSpec{id: 5, address: "10.0.0.1:50055"}},
		{spec: groupSpec{id: 2, address: "10.0.0.1:50052"}},
		{spec: groupSpec{id: 9, address: "10.0.0.1:50059"}},
	}
	got := canonicalSelfAddress("localhost:50051", runtimes)
	if got != "10.0.0.1:50052" {
		t.Fatalf("got %q, want lowest-group address 10.0.0.1:50052", got)
	}
}

func TestCanonicalSelfAddressFallsBackWithoutRuntimes(t *testing.T) {
	t.Parallel()
	got := canonicalSelfAddress("localhost:50051", nil)
	if got != "localhost:50051" {
		t.Fatalf("got %q, want fallback localhost:50051", got)
	}
}

func TestLoadAdminTokenFileRejectsOversize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", adminTokenMaxBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAdminTokenFile(path); err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected size-cap error, got %v", err)
	}
}
