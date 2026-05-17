package encryption_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
//
// The filename encodes the seed as 2-digit hex so a non-ASCII seed
// (e.g., 0xAA) does not become a 2-byte UTF-8 path component that
// would surprise readers of the on-disk test artifacts.
func newTestKEK(t *testing.T, dir string, seed byte) kek.Wrapper {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("kek-%02x.bin", seed))
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
	if got := err.Error(); !strings.Contains(got, "purpose=\"raft\"") {
		t.Errorf("KEK mismatch error %q must annotate purpose=raft for runbook grep", got)
	}
}

// TestCheckStartupGuards_KEKMismatch_DeterministicAnnotation pins
// claude r1 MEDIUM on PR #778: when more than one wrapped DEK
// fails to unwrap, the reported key_id MUST be the lowest one
// so the error annotation is reproducible across process restarts.
// Map-order iteration would pick a different DEK each restart,
// breaking runbook log correlation. We use raft=2 / storage=1 so
// that if iteration is by-insertion-order the wrong purpose
// surfaces; sorted iteration always picks key_id=1 (storage).
func TestCheckStartupGuards_KEKMismatch_DeterministicAnnotation(t *testing.T) {
	dir := t.TempDir()
	wrongKEK := newTestKEK(t, dir, 0xAA)
	// Both DEKs wrapped under wrongKEK from the perspective of the
	// configured KEK (rightKEK below) so EVERY DEK in the sidecar
	// fails to unwrap. The guard must always pick key_id=1
	// (storage), the smallest id.
	storageWrapped, err := wrongKEK.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap storage: %v", err)
	}
	raftWrapped, err := wrongKEK.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap raft: %v", err)
	}
	sidecarPath := writeTestSidecar(t, dir, storageWrapped, raftWrapped)

	rightKEK := newTestKEK(t, dir, 0x42)

	// Loop a few iterations to catch any non-determinism via
	// Go's randomized map iteration order. If the implementation
	// regresses to map-order, a single run might happen to pick
	// key_id=1 by coincidence; ten iterations make the bug
	// reliably reproducible.
	for i := 0; i < 10; i++ {
		err := encryption.CheckStartupGuards(encryption.StartupConfig{
			EncryptionEnabled: true,
			KEKConfigured:     true,
			KEK:               rightKEK,
			SidecarPath:       sidecarPath,
		})
		if !errors.Is(err, encryption.ErrKEKMismatch) {
			t.Fatalf("iter %d: expected ErrKEKMismatch, got %v", i, err)
		}
		if got := err.Error(); !strings.Contains(got, "key_id=1") {
			t.Fatalf("iter %d: KEK mismatch on multi-failure sidecar must always annotate key_id=1 (smallest), got %q", i, got)
		}
		if got := err.Error(); !strings.Contains(got, "purpose=\"storage\"") {
			t.Fatalf("iter %d: KEK mismatch on key_id=1 must annotate purpose=storage, got %q", i, got)
		}
	}
}

// TestCheckStartupGuards_EmptyWrappedSkipped pins the early-pass
// behaviour for a "bootstrap not yet committed" sidecar: the file
// exists on disk (e.g., touched by an out-of-band tool) and has
// SidecarKey entries but every Wrapped field is empty. The guard
// must skip empty-wrapped entries rather than calling KEK.Unwrap
// on zero bytes (which the KEK wrapper would reject as too short
// and fire a misleading ErrKEKMismatch).
//
// This locks down the godoc claim that the helper "Returns nil
// ... when ... the sidecar exists but has no wrapped DEKs".
func TestCheckStartupGuards_EmptyWrappedSkipped(t *testing.T) {
	dir := t.TempDir()
	// Write a sidecar with two SidecarKey entries that BOTH have
	// empty Wrapped bytes. validateSidecar requires Active.X to
	// either be 0 or point at a key in the map, so we leave the
	// Active fields at 0 (not bootstrapped for either purpose).
	sc := &encryption.Sidecar{
		Version: encryption.SidecarVersion,
		Keys: map[string]encryption.SidecarKey{
			"3": {Purpose: encryption.SidecarPurposeStorage, Wrapped: nil, Created: "2026-05-17T00:00:00Z"},
			"4": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte{}, Created: "2026-05-17T00:00:00Z"},
		},
	}
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	err := encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     true,
		KEK:               newTestKEK(t, dir, 0x42),
		SidecarPath:       sidecarPath,
	})
	if err != nil {
		t.Fatalf("expected nil for empty-Wrapped sidecar, got %v", err)
	}
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

// writeTestSidecarWithEpoch persists a §5.1 sidecar with the
// supplied storage active DEK and a chosen local_epoch value. Used
// by the local-epoch exhaustion tests to inject the saturated
// value without going through a real rotation.
func writeTestSidecarWithEpoch(t *testing.T, dir string, wrapped []byte, localEpoch uint16) string {
	t.Helper()
	sc := &encryption.Sidecar{
		Version: encryption.SidecarVersion,
		Keys: map[string]encryption.SidecarKey{
			"1": {
				Purpose:    encryption.SidecarPurposeStorage,
				Wrapped:    wrapped,
				Created:    "2026-05-17T00:00:00Z",
				LocalEpoch: localEpoch,
			},
		},
	}
	sc.Active.Storage = 1
	path := filepath.Join(dir, encryption.SidecarFilename)
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

// TestCheckStartupGuards_LocalEpochExhausted_Storage pins the
// Stage 6C-2 ErrLocalEpochExhausted guard. An active DEK with
// local_epoch == 0xFFFF is one bump away from rolling back to 0
// and re-issuing a nonce already in use under the same DEK —
// the classic GCM catastrophic key-reuse pattern. Refuse to
// start until the operator rotates.
func TestCheckStartupGuards_LocalEpochExhausted_Storage(t *testing.T) {
	dir := t.TempDir()
	k := newTestKEK(t, dir, 0x42)
	wrapped, err := k.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	sidecarPath := writeTestSidecarWithEpoch(t, dir, wrapped, 0xFFFF)
	err = encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     true,
		KEK:               k,
		SidecarPath:       sidecarPath,
	})
	if !errors.Is(err, encryption.ErrLocalEpochExhausted) {
		t.Fatalf("expected ErrLocalEpochExhausted, got %v", err)
	}
	if got := err.Error(); !strings.Contains(got, "purpose=\"storage\"") {
		t.Errorf("exhaustion error %q must annotate purpose=storage so the operator knows which DEK to rotate", got)
	}
}

// TestCheckStartupGuards_LocalEpochExhausted_BoundaryNotFire
// pins the saturation boundary: local_epoch == 0xFFFE MUST pass
// (one nonce still available), 0xFFFF MUST fire. A bug that uses
// >= 0xFFFE instead of == 0xFFFF would refuse a perfectly safe
// DEK and force a spurious rotation.
func TestCheckStartupGuards_LocalEpochExhausted_BoundaryNotFire(t *testing.T) {
	dir := t.TempDir()
	k := newTestKEK(t, dir, 0x42)
	wrapped, err := k.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	sidecarPath := writeTestSidecarWithEpoch(t, dir, wrapped, 0xFFFE)
	err = encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     true,
		KEK:               k,
		SidecarPath:       sidecarPath,
	})
	if errors.Is(err, encryption.ErrLocalEpochExhausted) {
		t.Fatalf("local_epoch=0xFFFE must NOT fire exhaustion guard (one nonce still available), got %v", err)
	}
	if err != nil {
		t.Fatalf("local_epoch=0xFFFE must pass cleanly, got %v", err)
	}
}

// TestCheckStartupGuards_LocalEpochExhausted_RetiredKeyIgnored
// verifies the guard only inspects ACTIVE DEKs. A retired DEK
// (active id points elsewhere) with local_epoch=0xFFFF is harmless
// — no new write will allocate a nonce under it.
func TestCheckStartupGuards_LocalEpochExhausted_RetiredKeyIgnored(t *testing.T) {
	dir := t.TempDir()
	k := newTestKEK(t, dir, 0x42)
	wrappedFresh, err := k.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap fresh: %v", err)
	}
	wrappedRetired, err := k.Wrap(make([]byte, 32))
	if err != nil {
		t.Fatalf("Wrap retired: %v", err)
	}
	// Build a sidecar where the active storage points at key_id=2
	// (fresh, local_epoch=0) and the retired DEK at key_id=1 has
	// local_epoch=0xFFFF. The guard must not fire.
	sc := &encryption.Sidecar{
		Version: encryption.SidecarVersion,
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: wrappedRetired, Created: "2026-05-17T00:00:00Z", LocalEpoch: 0xFFFF},
			"2": {Purpose: encryption.SidecarPurposeStorage, Wrapped: wrappedFresh, Created: "2026-05-17T00:00:00Z", LocalEpoch: 0},
		},
	}
	sc.Active.Storage = 2
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	if err := encryption.WriteSidecar(sidecarPath, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	err = encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     true,
		KEK:               k,
		SidecarPath:       sidecarPath,
	})
	if err != nil {
		t.Fatalf("retired DEK with exhausted local_epoch must NOT fire guard, got %v", err)
	}
}

// TestProbeSidecarFilesystem_OK exercises the happy path: a tmp
// directory on the host filesystem supports the write+rename+dir.Sync
// sequence the §5.1 protocol requires, so the probe returns nil
// and leaves no sentinel file behind.
func TestProbeSidecarFilesystem_OK(t *testing.T) {
	dir := t.TempDir()
	sidecarPath := filepath.Join(dir, "keys.json")
	if err := encryption.ProbeSidecarFilesystem(sidecarPath); err != nil {
		t.Fatalf("probe on tmp dir must succeed, got %v", err)
	}
	// Probe must leave no sentinel files behind on success.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".encryption-probe-") {
			t.Errorf("probe left behind a sentinel file on success: %s", e.Name())
		}
	}
}

// TestProbeSidecarFilesystem_MissingDir verifies that a missing
// parent directory propagates as a real OS error (not wrapped with
// ErrUnsupportedFilesystem). The two failure classes are distinct:
// missing dir is a config error the operator must fix; filesystem
// no-fsync is an environment problem that needs a different mount.
func TestProbeSidecarFilesystem_MissingDir(t *testing.T) {
	err := encryption.ProbeSidecarFilesystem("/nonexistent/elastickv-probe/keys.json")
	if err == nil {
		t.Fatal("expected error for missing parent directory")
	}
	if errors.Is(err, encryption.ErrUnsupportedFilesystem) {
		t.Errorf("missing dir must NOT be classified as ErrUnsupportedFilesystem (config error vs FS-capability): %v", err)
	}
}

// TestCheckStartupGuards_FilesystemProbe_RunsOnlyWhenEnabled
// verifies that the filesystem probe is gated on
// --encryption-enabled. A cluster that has NOT opted into the
// §7.1 rollout has no encryption-relevant write to worry about
// crash-durability for, so a non-fsync filesystem in the
// sidecar dir is irrelevant — the probe must skip silently.
func TestCheckStartupGuards_FilesystemProbe_RunsOnlyWhenEnabled(t *testing.T) {
	// SidecarPath set, EncryptionEnabled=false → probe should
	// not run (and even if it did, we'd hit the missing-dir
	// branch rather than ErrUnsupportedFilesystem). Use a path
	// whose parent does NOT exist to make any probe attempt
	// fail loudly.
	err := encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: false,
		SidecarPath:       "/nonexistent/elastickv-probe-disabled/keys.json",
	})
	if err != nil {
		t.Fatalf("filesystem probe must skip when --encryption-enabled is off, got %v", err)
	}
}

// TestCheckStartupGuards_CorruptSidecarMissingKEK_RefusesWithKEKError
// pins the claude-r2 / codex-P2 error-precedence guarantee: when
// BOTH the sidecar is unreadable AND --kekFile is missing, the
// operator MUST see ErrKEKRequiredWithFlag (their primary
// misconfiguration), not a downstream sidecar-parse error. The
// loadSidecarForGuards KEK-missing short-circuit is what enforces
// this ordering; without it the sidecar would be read first and
// the parse error would mask the missing-KEK condition.
func TestCheckStartupGuards_CorruptSidecarMissingKEK_RefusesWithKEKError(t *testing.T) {
	dir := t.TempDir()
	// Write a malformed sidecar — version=0 fails the ReadSidecar
	// validation. Any unreadable file would do; we use malformed
	// rather than missing because os.Stat distinguishes the two
	// and "missing" wouldn't reach the ReadSidecar path at all.
	sidecarPath := filepath.Join(dir, encryption.SidecarFilename)
	if err := os.WriteFile(sidecarPath, []byte(`{"version":0}`), 0o600); err != nil {
		t.Fatalf("write malformed sidecar: %v", err)
	}
	err := encryption.CheckStartupGuards(encryption.StartupConfig{
		EncryptionEnabled: true,
		KEKConfigured:     false, // <-- the missing-KEK condition
		SidecarPath:       sidecarPath,
	})
	if !errors.Is(err, encryption.ErrKEKRequiredWithFlag) {
		t.Fatalf("missing-KEK MUST take precedence over sidecar-parse error; got %v", err)
	}
}

// TestCheckStartupGuards_KEKMismatch_ParseErrIncluded pins the
// gemini medium finding deferred from PR #778 r3: when ParseUint
// fails in the defensive non-decimal-key_id branch, the
// ErrKEKMismatch annotation MUST include parseErr so an operator
// triaging a manually-edited sidecar can see BOTH the unwrap
// failure and the malformed key_id reason. validateSidecar
// normally rejects non-decimal keys at ReadSidecar time, so this
// branch is defensive-only; we cannot trigger it through the
// public API without bypassing validation. The assertion here
// pins the format string of the parseErr-included path.
func TestCheckStartupGuards_KEKMismatch_ParseErrIncluded(t *testing.T) {
	// Sanity: the format string from startup.go must reference
	// "parse error:" in the parseErr-included branch. Grepping
	// the package source rather than executing the unreachable
	// branch keeps the test deterministic without forcing us
	// to inject invalid sidecars.
	src, err := os.ReadFile("startup.go")
	if err != nil {
		t.Fatalf("ReadFile startup.go: %v", err)
	}
	if !strings.Contains(string(src), `"sidecar=%q key_id=%q (parse error: %v): %v"`) {
		t.Error("ErrKEKMismatch parseErr-included format string missing — gemini r3 medium regression")
	}
}
