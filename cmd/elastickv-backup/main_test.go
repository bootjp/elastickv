package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/backup"
)

func TestParseFlagsScopesBundleAndArchive(t *testing.T) {
	t.Parallel()
	cfg, err := parseFlags([]string{
		"dump",
		"--address", "127.0.0.1:50051",
		"--output-dir", "/tmp/dump",
		"--output-format", "tar+zstd",
		"--adapter", "dynamodb,s3",
		"--scope", "dynamodb=users,orders",
		"--scope", "s3=photos",
		"--dynamodb-bundle-size", "2MiB",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.format != "tar+zstd" || cfg.bundleJSONL || cfg.bundleSizeBytes != 2<<20 {
		t.Fatalf("cfg=%+v", cfg)
	}
	want := []backup.Scope{
		{Adapter: "dynamodb", Name: "orders"},
		{Adapter: "dynamodb", Name: "users"},
		{Adapter: "s3", Name: "photos"},
	}
	if len(cfg.scopes) != len(want) {
		t.Fatalf("scopes=%v", cfg.scopes)
	}
	for i := range want {
		if cfg.scopes[i] != want[i] {
			t.Fatalf("scope[%d]=%v, want %v", i, cfg.scopes[i], want[i])
		}
	}
}

func TestParseFlagsRejectsModesUnsupportedByNativeRestore(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"--adapter", "dynamodb", "--dynamodb-bundle-mode", "jsonl"},
		{"--adapter", "s3", "--include-incomplete-uploads"},
		{"--adapter", "s3", "--include-orphans"},
		{"--adapter", "sqs", "--preserve-sqs-visibility"},
		{"--adapter", "sqs", "--include-sqs-side-records"},
	}
	for _, extra := range cases {
		argv := []string{"dump", "--address", "127.0.0.1:50051", "--output-dir", "/tmp/dump"}
		_, err := parseFlags(append(argv, extra...))
		if !errors.Is(err, backup.ErrLiveBackupRestoreUnsupported) {
			t.Fatalf("flags=%v error=%v, want ErrLiveBackupRestoreUnsupported", extra, err)
		}
	}
}

func TestParseFlagsRejectsScopeExcludedByAdapter(t *testing.T) {
	t.Parallel()
	_, err := parseFlags([]string{
		"dump", "--address", "127.0.0.1:50051", "--output-dir", "/tmp/dump",
		"--adapter", "redis", "--scope", "s3=photos",
	})
	if err == nil {
		t.Fatal("parseFlags accepted excluded scope")
	}
}

func TestParseByteSize(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]int64{
		"1": 1, "1B": 1, "2KiB": 2 << 10, "3MiB": 3 << 20, "4GiB": 4 << 30,
	} {
		got, err := parseByteSize(input)
		if err != nil || got != want {
			t.Fatalf("parseByteSize(%q)=(%d,%v), want %d", input, got, err, want)
		}
	}
}

func TestLoadTransportCredentialsRejectsIgnoredTLSFlags(t *testing.T) {
	t.Parallel()
	if _, err := loadTransportCredentials("", "backup.internal", false); err == nil || !strings.Contains(err.Error(), "requires") {
		t.Fatalf("server name without TLS error = %v", err)
	}
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, []byte("not used"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransportCredentials(caPath, "", true); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("CA plus skip-verify error = %v", err)
	}
}
