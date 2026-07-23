package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinalizeDumpChecksumsManifestAndPublishesItLast(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "data.txt"), []byte("value"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	manifest := NewPhase0SnapshotManifest(time.Now())
	manifest.Phase = PhasePhase1LivePinned
	manifest.Live = &Live{ReadTS: 42}

	if err := FinalizeDump(root, manifest); err != nil {
		t.Fatalf("FinalizeDump: %v", err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatalf("VerifyChecksums: %v", err)
	}
	checksums, err := os.ReadFile(filepath.Join(root, CHECKSUMSFilename)) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(checksums), "  "+ManifestFilename+"\n") {
		t.Fatalf("CHECKSUMS does not include manifest: %s", checksums)
	}
}

func TestFinalizeDumpInvalidManifestLeavesNoCommitMarker(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	manifest := NewPhase0SnapshotManifest(time.Now())
	manifest.Phase = PhasePhase1LivePinned

	if err := FinalizeDump(root, manifest); err == nil {
		t.Fatal("FinalizeDump accepted phase1 manifest without live metadata")
	}
	if _, err := os.Lstat(filepath.Join(root, ManifestFilename)); !os.IsNotExist(err) {
		t.Fatalf("MANIFEST exists after failure: %v", err)
	}
}

func TestFinalizeDumpRefusesExistingManifest(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	manifestPath := filepath.Join(root, ManifestFilename)
	existing := []byte("existing manifest")
	if err := os.WriteFile(manifestPath, existing, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	manifest := NewPhase0SnapshotManifest(time.Now())
	manifest.Phase = PhasePhase1LivePinned
	manifest.Live = &Live{ReadTS: 42}
	if err := FinalizeDump(root, manifest); err == nil {
		t.Fatal("FinalizeDump replaced an existing manifest")
	}
	got, err := os.ReadFile(manifestPath) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(existing) {
		t.Fatalf("manifest content = %q, want %q", got, existing)
	}
}
