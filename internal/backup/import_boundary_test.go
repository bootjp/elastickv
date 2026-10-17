package backup

import (
	"go/build"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const elastickvModule = "github.com/bootjp/elastickv"

// backupForbiddenDeps are the live-cluster packages the offline backup
// tools must not link. The adapter encoders backup needs are mirrored
// here and cross-checked against the live code by tests instead
// (docs/design/2026_05_25_implemented_snapshot_logical_encoder.md).
// proto and the leaf key packages (internal/s3keys) stay allowed.
var backupForbiddenDeps = []string{
	elastickvModule + "/store",
	elastickvModule + "/adapter",
	elastickvModule + "/kv",
	elastickvModule + "/distribution",
	elastickvModule + "/internal/encryption",
	"github.com/cockroachdb/pebble",
}

// backupAllowedDeps are exact package paths exempt from
// backupForbiddenDeps. Snapshot SST ingest bundles are Pebble sstable
// files, so the decoder reads them with Pebble's sstable reader
// (snapshot_sst.go, docs/design/2026_04_29_implemented_snapshot_logical_decoder.md).
// That package cannot reach the Pebble engine, which imports it, and
// importing the engine or any other Pebble package still fails the test.
var backupAllowedDeps = []string{
	"github.com/cockroachdb/pebble/v2/sstable",
}

// TestBackupLinksNoLiveClusterCode walks the non-test import closure of
// internal/backup through first-party packages and fails on any
// forbidden package, naming the import chain that reaches it.
func TestBackupLinksNoLiveClusterCode(t *testing.T) {
	t.Parallel()

	root := moduleRoot(t)
	start := elastickvModule + "/internal/backup"
	via := map[string]string{start: ""}
	queue := []string{start}
	for len(queue) > 0 {
		pkgPath := queue[0]
		queue = queue[1:]
		dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(pkgPath, elastickvModule)))
		pkg, err := build.ImportDir(dir, 0)
		if err != nil {
			t.Fatalf("read package %s: %v", pkgPath, err)
		}
		for _, imp := range pkg.Imports {
			if _, seen := via[imp]; seen {
				continue
			}
			via[imp] = pkgPath
			if forbidden := forbiddenBackupDep(imp); forbidden != "" {
				t.Errorf("internal/backup links %s: %s", forbidden, importChain(via, imp))
				continue
			}
			if imp == elastickvModule || strings.HasPrefix(imp, elastickvModule+"/") {
				queue = append(queue, imp)
			}
		}
	}
	for _, allowed := range backupAllowedDeps {
		if _, seen := via[allowed]; !seen {
			t.Errorf("internal/backup no longer links %s; remove it from backupAllowedDeps", allowed)
		}
	}
}

// TestForbiddenBackupDep pins the boundary's matching rules: a forbidden
// entry covers its subpackages, and an allowed entry exempts only its
// exact path.
func TestForbiddenBackupDep(t *testing.T) {
	t.Parallel()

	const pebble = "github.com/cockroachdb/pebble"
	cases := []struct {
		pkgPath string
		want    string
	}{
		{pkgPath: pebble + "/v2/sstable", want: ""},
		{pkgPath: pebble + "/v2", want: pebble},
		{pkgPath: pebble + "/v2/sstable/block", want: pebble},
		{pkgPath: pebble + "/sstable", want: pebble},
		{pkgPath: elastickvModule + "/store", want: elastickvModule + "/store"},
		{pkgPath: elastickvModule + "/internal/encryption/kek", want: elastickvModule + "/internal/encryption"},
		{pkgPath: elastickvModule + "/internal/s3keys", want: ""},
	}
	for _, tc := range cases {
		if got := forbiddenBackupDep(tc.pkgPath); got != tc.want {
			t.Errorf("forbiddenBackupDep(%q) = %q, want %q", tc.pkgPath, got, tc.want)
		}
	}
}

func forbiddenBackupDep(pkgPath string) string {
	if slices.Contains(backupAllowedDeps, pkgPath) {
		return ""
	}
	for _, forbidden := range backupForbiddenDeps {
		if pkgPath == forbidden || strings.HasPrefix(pkgPath, forbidden+"/") {
			return forbidden
		}
	}
	return ""
}

func importChain(via map[string]string, pkgPath string) string {
	chain := []string{pkgPath}
	for parent := via[pkgPath]; parent != ""; parent = via[parent] {
		chain = append([]string{parent}, chain...)
	}
	return strings.Join(chain, " -> ")
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}
