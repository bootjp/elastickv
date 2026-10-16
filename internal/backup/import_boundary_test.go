package backup

import (
	"go/build"
	"os"
	"path/filepath"
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
}

func forbiddenBackupDep(pkgPath string) string {
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
