package main

import (
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
		"--dynamodb-bundle-mode", "jsonl",
		"--dynamodb-bundle-size", "2MiB",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.format != "tar+zstd" || !cfg.bundleJSONL || cfg.bundleSizeBytes != 2<<20 {
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
