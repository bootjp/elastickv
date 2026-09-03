package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

// countdownCancelContext reports "not canceled" for the first n Err calls and
// canceled after that, so a test can land the cancellation on a specific poll
// -- here, the one between checksumming and the manifest rename.
type countdownCancelContext struct {
	context.Context
	left int
	done chan struct{}
}

func newCountdownCancelContext(n int) *countdownCancelContext {
	return &countdownCancelContext{Context: context.Background(), left: n, done: make(chan struct{})}
}

func (c *countdownCancelContext) Err() error {
	if c.left > 0 {
		c.left--
		return nil
	}
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	return context.Canceled
}

func (c *countdownCancelContext) Done() <-chan struct{} { return c.done }

// A canceled run must not leave a manifest behind. Checksumming reads every
// byte of the dump, so cancellation arriving during it has to stop the
// publication that follows -- the caller's preflight check ran long before.
func TestFinalizeDumpContextStopsOnCancellation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "part-0000"), []byte("payload"), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := FinalizeDumpContext(ctx, root, validLiveManifest())
	require.ErrorIs(t, err, context.Canceled)
	requireNoManifest(t, root)
}

// Cancellation that lands after the checksum walk but before the rename must
// still block publication: that rename is what makes the dump count as
// complete.
func TestFinalizeDumpContextRechecksBeforePublishing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "part-0000"), []byte("payload"), 0o600))

	// One poll per walked file, then the pre-rename poll is the canceled one.
	ctx := newCountdownCancelContext(1)
	err := FinalizeDumpContext(ctx, root, validLiveManifest())
	require.ErrorIs(t, err, context.Canceled)
	requireNoManifest(t, root)
}

func validLiveManifest() Manifest {
	manifest := NewPhase0SnapshotManifest(time.Now())
	manifest.Phase = PhasePhase1LivePinned
	manifest.Live = &Live{ReadTS: 42}
	return manifest
}

func requireNoManifest(t *testing.T, root string) {
	t.Helper()
	_, err := os.Lstat(filepath.Join(root, ManifestFilename))
	require.ErrorIs(t, err, os.ErrNotExist, "a canceled run must not publish a manifest")
}
