package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/adapter"
)

func TestConfigureAdminServiceDisabledByDefault(t *testing.T) {
	t.Parallel()
	srv, opts, err := configureAdminService("", false, adapter.NodeIdentity{NodeID: "n1"})
	if err != nil {
		t.Fatalf("disabled-by-default should not error: %v", err)
	}
	if srv != nil || opts != nil {
		t.Fatalf("disabled service should return nil, nil; got %v %v", srv, opts)
	}
}

func TestConfigureAdminServiceRejectsMutualExclusion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	tokPath := filepath.Join(dir, "t")
	if err := os.WriteFile(tokPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := configureAdminService(tokPath, true, adapter.NodeIdentity{}); err == nil {
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
	srv, opts, err := configureAdminService(tokPath, false, adapter.NodeIdentity{NodeID: "n1"})
	if err != nil {
		t.Fatalf("configureAdminService: %v", err)
	}
	if srv == nil {
		t.Fatal("expected an AdminServer instance")
	}
	// Expect a unary + stream interceptor for the admin-token gate.
	if len(opts) != 2 {
		t.Fatalf("expected 2 grpc.ServerOption (unary + stream), got %d", len(opts))
	}
}

func TestConfigureAdminServiceInsecureNoAuth(t *testing.T) {
	t.Parallel()
	srv, opts, err := configureAdminService("", true, adapter.NodeIdentity{NodeID: "n1"})
	if err != nil {
		t.Fatalf("insecure mode should succeed: %v", err)
	}
	if srv == nil {
		t.Fatal("expected AdminServer in insecure mode")
	}
	if len(opts) != 0 {
		t.Fatalf("insecure mode should not attach interceptors, got %d", len(opts))
	}
}

func TestLoadAdminTokenFileRejectsOversize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", adminTokenMaxBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAdminTokenFile(path); err == nil || !strings.Contains(err.Error(), "maximum is") {
		t.Fatalf("expected size-cap error, got %v", err)
	}
}
