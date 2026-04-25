package internal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadBearerTokenFileHappyPath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "tok")
	if err := os.WriteFile(path, []byte("\n  s3cret \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadBearerTokenFile(path, 4<<10, "admin token")
	if err != nil {
		t.Fatalf("LoadBearerTokenFile: %v", err)
	}
	if got != "s3cret" {
		t.Fatalf("tok = %q, want s3cret", got)
	}
}

func TestLoadBearerTokenFileRejectsEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")
	if err := os.WriteFile(path, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadBearerTokenFile(path, 4<<10, "admin token")
	if err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("want empty-file error, got %v", err)
	}
}

func TestLoadBearerTokenFileRejectsOversize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "huge")
	const cap_ = 64
	if err := os.WriteFile(path, []byte(strings.Repeat("x", cap_+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadBearerTokenFile(path, cap_, "admin token")
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("want oversize error, got %v", err)
	}
}

func TestLoadBearerTokenFileMissingFile(t *testing.T) {
	t.Parallel()
	_, err := LoadBearerTokenFile("/definitely/not/there", 4<<10, "admin token")
	if err == nil {
		t.Fatal("expected open-failure error")
	}
}
