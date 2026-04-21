package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSplitNodesTrimsAndDrops(t *testing.T) {
	t.Parallel()
	got := splitNodes(" host-a:50051 ,,host-b:50051 ,")
	want := []string{"host-a:50051", "host-b:50051"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestLoadTokenRequiresFileOrInsecure(t *testing.T) {
	t.Parallel()
	if _, err := loadToken("", false); err == nil {
		t.Fatal("expected error when neither token nor insecure mode supplied")
	}
	tok, err := loadToken("", true)
	if err != nil {
		t.Fatalf("insecure-mode empty path should succeed: %v", err)
	}
	if tok != "" {
		t.Fatalf("insecure-mode token = %q, want empty", tok)
	}
}

func TestLoadTokenReadsAndTrims(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("\n  s3cret \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tok, err := loadToken(path, false)
	if err != nil {
		t.Fatalf("loadToken: %v", err)
	}
	if tok != "s3cret" {
		t.Fatalf("tok = %q, want s3cret", tok)
	}
}

func TestLoadTokenRejectsEmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")
	if err := os.WriteFile(path, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := loadToken(path, false)
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected empty-file error, got %v", err)
	}
}

func TestLoadTokenRejectsInsecureWithFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "tok")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadToken(path, true); err == nil {
		t.Fatal("expected mutual-exclusion error when both supplied")
	}
}
