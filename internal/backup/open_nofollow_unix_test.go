//go:build unix

package backup

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpenSidecarFileEnforcesOwnerOnlyMode pins claude / codex P2 v31
// observation on PR #904: an older encoder may have written the
// sidecar at 0o644; OpenFile's mode arg only applies on CREATE, so
// re-opening for re-encode would preserve the wider perms. The
// post-Truncate Chmod restores 0o600 on every successful open.
func TestOpenSidecarFileEnforcesOwnerOnlyMode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "sidecar.json")
	// Pre-existing sidecar with wider perms (simulating an older
	// encoder).
	if err := os.WriteFile(path, []byte("prior"), 0o644); err != nil { //nolint:gosec // test simulates legacy permissive sidecar
		t.Fatalf("WriteFile: %v", err)
	}
	// Verify the environment actually honored the broader seed mode;
	// a restrictive umask or stricter FS could silently produce 0o600
	// and the test would pass even if the chmod-enforcement logic
	// regressed (CodeRabbit nit on PR #904).
	seedInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat seeded file: %v", err)
	}
	if seedInfo.Mode().Perm()&0o077 == 0 {
		t.Skipf("environment refused permissive seed mode (got 0o%o); test cannot exercise chmod-enforcement", seedInfo.Mode().Perm())
	}
	f, err := OpenSidecarFile(path)
	if err != nil {
		t.Fatalf("OpenSidecarFile: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("perm = %o, want 0o600 (Chmod after Truncate must tighten existing-file perms)", got)
	}
}
