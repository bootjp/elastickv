package encryption_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
)

// writeTestSidecar persists a §5.1 sidecar to <dir>/keys.json with
// the supplied storage / raft wrapped DEK material. Either side may
// be nil to leave that purpose unbootstrapped. Returns the path.
func writeTestSidecar(t *testing.T, dir string, storageWrapped, raftWrapped []byte) string {
	t.Helper()
	sc := &encryption.Sidecar{
		Version: encryption.SidecarVersion,
		Keys:    map[string]encryption.SidecarKey{},
	}
	if storageWrapped != nil {
		sc.Keys["1"] = encryption.SidecarKey{
			Purpose:    encryption.SidecarPurposeStorage,
			Wrapped:    storageWrapped,
			Created:    "2026-05-17T00:00:00Z",
			LocalEpoch: 0,
		}
		sc.Active.Storage = 1
	}
	if raftWrapped != nil {
		sc.Keys["2"] = encryption.SidecarKey{
			Purpose:    encryption.SidecarPurposeRaft,
			Wrapped:    raftWrapped,
			Created:    "2026-05-17T00:00:00Z",
			LocalEpoch: 0,
		}
		sc.Active.Raft = 2
	}
	path := filepath.Join(dir, encryption.SidecarFilename)
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

// newTestKEK constructs a KEK wrapper backed by a 32-byte file in
// dir. The file is filled with the supplied seed so two test KEKs
// constructed from different seeds will not be able to unwrap each
// other's wrapped DEKs (which is what the ErrKEKMismatch test needs).
func newTestKEK(t *testing.T, dir string, seed byte) kek.Wrapper {
	t.Helper()
	path := filepath.Join(dir, "kek-"+string(rune(seed))+".bin")
	material := make([]byte, 32)
	for i := range material {
		material[i] = seed
	}
	if err := os.WriteFile(path, material, 0o600); err != nil {
		t.Fatalf("write KEK seed: %v", err)
	}
	w, err := kek.NewFileWrapper(path)
	if err != nil {
		t.Fatalf("NewFileWrapper: %v", err)
	}
	return w
}

// TestCheckStartupGuards_OK exercises the "everything is correctly
// configured, nothing to refuse" paths. Each sub-case represents a
// legitimate startup posture that MUST be allowed.
func TestCheckStartupGuards_OK(t *testing.T) {
	t.Run("no_flag_no_sidecar", func(t *testing.T) {
		// Plain unencrypted operation: flag off, no sidecar path
		// supplied. This is the production posture for clusters that
		// haven't opted in.
		err := encryption.CheckStartupGuards(encryption.StartupConfig{})
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("no_flag_sidecar_path_but_file_absent", func(t *testing.T) {
		// Operator supplied --encryptionSidecarPath for forward-
		// compatibility but never enabled encryption; no sidecar
		// file exists on disk → nothing to refuse.
		dir := t.TempDir()
		err := encryption.CheckStartupGuards(encryption.StartupConfig{
			SidecarPath: filepath.Join(dir, "keys.json"),
		})
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("flag_on_kek_loaded_no_sidecar_yet", func(t *testing.T) {
		// Fresh cluster mid-rollout: operator has set both
		// --encryption-enabled and --kekFile but has not yet
		// committed the §5.6 bootstrap entry, so no sidecar exists
		// on disk. Startup must pass; the bootstrap RPC will
		// create the sidecar on commit.
		dir := t.TempDir()
		err := encryption.CheckStartupGuards(encryption.StartupConfig{
			EncryptionEnabled: true,
			KEKConfigured:     true,
			KEK:               newTestKEK(t, dir, 0x42),
			SidecarPath:       filepath.Join(dir, "keys.json"),
		})
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("flag_on_matching_kek", func(t *testing.T) {
		// Steady-state encrypted operation: sidecar present, KEK
		// loaded, and KEK successfully unwraps every wrapped DEK.
		dir := t.TempDir()
		k := newTestKEK(t, dir, 0x42)
		storageWrapped, err := k.Wrap(make([]byte, 32))
		if err != nil {
			t.Fatalf("Wrap storage DEK: %v", err)
		}
		raftWrapped, err := k.Wrap(make([]byte, 32))
		if err != nil {
			t.Fatalf("Wrap raft DEK: %v", err)
		}
		sidecarPath := writeTestSidecar(t, dir, storageWrapped, raftWrapped)
		err = encryption.CheckStartupGuards(encryption.StartupConfig{
			EncryptionEnabled: true,
			KEKConfigured:     true,
			KEK:               k,
			SidecarPath:       sidecarPath,
		})
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})
}

// TestCheckStartupGuards_SidecarPresentWithoutFlag pins the
// downgrade-prevention guard: a node with on-disk encryption state
// but --encryption-enabled OFF MUST refuse to start. The classic
// failure mode this catches is an operator who toggled the flag
// off (intentionally or by accident) without cleaning the data
// dir first, which would silently route new writes to cleartext
// while old wrapped DEKs sit untouched on disk.
func TestCheckStartupGuards_SidecarPresentWithoutFlag(t *testing.T) {
	dir := t.TempDir()
	k := newTestKEK(t, dir, 0x42)
	wrapped, err := k.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	sidecarPath := writeTestSidecar(t, dir, wrapped, nil)
	err = encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: false,
		SidecarPath:       sidecarPath,
	})
	if !errors.Is(err, encryption.ErrSidecarPresentWithoutFlag) {
		t.Fatalf("expected ErrSidecarPresentWithoutFlag, got %v", err)
	}
}

// TestCheckStartupGuards_KEKRequiredWithFlag pins the
// flag-on / KEK-off misconfiguration guard. The triple gate in
// Stage 6B-2 already prevents the mutator-RPC HaltApply path,
// but a flag-on / KEK-off node is misconfigured at the
// operator-intent level and continuing would mislead a sleepy
// on-caller into thinking the cluster is encryption-capable.
func TestCheckStartupGuards_KEKRequiredWithFlag(t *testing.T) {
	dir := t.TempDir()
	// Sidecar path is supplied but the file does not exist; what
	// matters is the (flag, KEKConfigured) pair.
	err := encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     false,
		SidecarPath:       filepath.Join(dir, "keys.json"),
	})
	if !errors.Is(err, encryption.ErrKEKRequiredWithFlag) {
		t.Fatalf("expected ErrKEKRequiredWithFlag, got %v", err)
	}
}

// TestCheckStartupGuards_KEKMismatch pins the
// wrong-KEK-for-this-data-dir guard. The scenario: operator
// bootstrapped the cluster with KEK A, then restarted the
// process with --kekFile pointing at KEK B (from a different
// environment, or a backup KEK they grabbed by mistake). Without
// this guard the first write would succeed-and-vanish (encrypted
// under KEK B's wrap of a freshly-rotated DEK that the post-rotate
// reads can never recover the prior values for).
func TestCheckStartupGuards_KEKMismatch(t *testing.T) {
	dir := t.TempDir()
	wrappingKEK := newTestKEK(t, dir, 0x42)
	wrappedUnderA, err := wrappingKEK.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	sidecarPath := writeTestSidecar(t, dir, wrappedUnderA, nil)

	wrongKEK := newTestKEK(t, dir, 0xAA)
	err = encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     true,
		KEK:               wrongKEK,
		SidecarPath:       sidecarPath,
	})
	if !errors.Is(err, encryption.ErrKEKMismatch) {
		t.Fatalf("expected ErrKEKMismatch, got %v", err)
	}
}

// TestCheckStartupGuards_KEKMismatch_RaftOnly verifies the
// per-key error annotation surfaces the offending purpose
// (storage vs raft). Without that detail, root-causing a
// production mismatch would require an operator to manually
// re-derive which DEK failed.
func TestCheckStartupGuards_KEKMismatch_RaftOnly(t *testing.T) {
	dir := t.TempDir()
	rightKEK := newTestKEK(t, dir, 0x42)
	wrongKEK := newTestKEK(t, dir, 0xAA)
	storageWrapped, err := rightKEK.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap storage: %v", err)
	}
	raftWrapped, err := wrongKEK.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap raft: %v", err)
	}
	sidecarPath := writeTestSidecar(t, dir, storageWrapped, raftWrapped)

	// rightKEK can unwrap storage but not raft → mismatch fires
	// against the raft DEK.
	err = encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     true,
		KEK:               rightKEK,
		SidecarPath:       sidecarPath,
	})
	if !errors.Is(err, encryption.ErrKEKMismatch) {
		t.Fatalf("expected ErrKEKMismatch, got %v", err)
	}
	// The mismatch detail must mention purpose=raft so the
	// operator's log line points at the right DEK. The exact
	// format is intentionally part of the public contract here
	// because runbooks grep for it.
	if got := err.Error(); !contains(got, "purpose=\"raft\"") {
		t.Errorf("KEK mismatch error %q must annotate purpose=raft for runbook grep", got)
	}
}

// contains is a small substring helper to keep the test free of
// fmt/strings imports at this depth. Bringing in strings here
// would clutter the import diff for one assertion.
func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestCheckStartupGuards_SidecarStatError exercises the I/O
// error propagation path: a sidecar path that points into an
// unreadable directory should surface a wrapped error rather
// than being silently treated as "no sidecar". Otherwise a
// transient permission glitch could be misclassified as
// "downgrade is safe."
func TestCheckStartupGuards_SidecarStatError(t *testing.T) {
	// A path that contains a NUL byte will make os.Stat fail
	// with EINVAL on every platform we support, without
	// requiring the test to set up perms or chroot. The exact
	// underlying error code is platform-specific; we only check
	// that it does NOT collapse to "file does not exist".
	err := encryption.CheckStartupGuards(encryption.StartupConfig{
		SidecarPath: "/tmp/elastickv-startup-guards-test/\x00invalid",
	})
	if err == nil {
		t.Fatal("expected non-nil error for invalid sidecar path")
	}
	if errors.Is(err, encryption.ErrSidecarPresentWithoutFlag) {
		t.Errorf("invalid path must NOT be classified as downgrade-prevention: %v", err)
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Errorf("invalid path must NOT be silently treated as not-exist: %v", err)
	}
}
