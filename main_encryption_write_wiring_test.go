package main

import (
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

// wiringFakeKEK is a deterministic KEKUnwrapper for the write-wiring
// tests: it pads/truncates the wrapped input to KeySize so the same
// wrapped bytes always yield the same 32-byte DEK.
type wiringFakeKEK struct{}

func (wiringFakeKEK) Unwrap(wrapped []byte) ([]byte, error) {
	out := make([]byte, encryption.KeySize)
	copy(out, wrapped)
	return out, nil
}

func writeActiveStorageSidecar(t *testing.T, storageEpoch uint16) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/keys.json"
	sc := &encryption.Sidecar{
		Version:               encryption.SidecarVersion,
		StorageEnvelopeActive: true,
		Active:                encryption.ActiveKeys{Storage: 3, Raft: 4},
		Keys: map[string]encryption.SidecarKey{
			"3": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage-wrapped-bytes"), LocalEpoch: storageEpoch},
			"4": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft-wrapped-bytes-diff"), LocalEpoch: 0},
		},
	}
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

func TestEncryptionWriteWiring_PebbleOptions_CleartextWhenNoCipher(t *testing.T) {
	t.Parallel()
	if opts := (encryptionWriteWiring{}).pebbleOptions(); opts != nil {
		t.Errorf("zero wiring pebbleOptions = %d opts, want nil (cleartext)", len(opts))
	}
	// cache present but cipher nil still means cleartext.
	w := encryptionWriteWiring{cache: encryption.NewStateCache()}
	if opts := w.pebbleOptions(); opts != nil {
		t.Errorf("cipher-less wiring pebbleOptions = %d opts, want nil", len(opts))
	}
}

func TestEncryptionWriteWiring_PebbleOptions_WiredWhenCipherSet(t *testing.T) {
	t.Parallel()
	cipher, err := encryption.NewCipher(encryption.NewKeystore())
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	w := encryptionWriteWiring{
		cache:        encryption.NewStateCache(),
		cipher:       cipher,
		nonceFactory: encryption.NewDeterministicNonceFactory(0xABCD, 0),
	}
	if opts := w.pebbleOptions(); len(opts) != 3 {
		t.Errorf("wired pebbleOptions = %d opts, want 3 (WithEncryption + WithStorageEnvelopeGate + WithStorageRegistrationGate)", len(opts))
	}
}

func TestPrepareStorageNonceEpoch_NoSidecar_ReturnsZero(t *testing.T) {
	t.Parallel()
	cache := encryption.NewStateCache()
	epoch, err := prepareStorageNonceEpoch(t.TempDir()+"/absent.json", wiringFakeKEK{}, encryption.NewKeystore(), cache)
	if err != nil {
		t.Fatalf("prepareStorageNonceEpoch: %v", err)
	}
	if epoch != 0 {
		t.Errorf("epoch = %d, want 0 (no sidecar / pre-bootstrap)", epoch)
	}
	if _, ok := cache.ActiveStorageKeyID(); ok {
		t.Error("cache reports an active DEK with no sidecar")
	}
}

func TestPrepareStorageNonceEpoch_NotBootstrapped_ReturnsZero(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := dir + "/keys.json"
	// Sidecar with no active storage DEK (Active.Storage == 0).
	if err := encryption.WriteSidecar(path, &encryption.Sidecar{Version: encryption.SidecarVersion}); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	cache := encryption.NewStateCache()
	epoch, err := prepareStorageNonceEpoch(path, wiringFakeKEK{}, encryption.NewKeystore(), cache)
	if err != nil {
		t.Fatalf("prepareStorageNonceEpoch: %v", err)
	}
	if epoch != 0 {
		t.Errorf("epoch = %d, want 0 (Active.Storage == 0)", epoch)
	}
}

func TestPrepareStorageNonceEpoch_ActiveDEK_HydratesAndBumps(t *testing.T) {
	t.Parallel()
	path := writeActiveStorageSidecar(t, 6)
	ks := encryption.NewKeystore()
	cache := encryption.NewStateCache()
	epoch, err := prepareStorageNonceEpoch(path, wiringFakeKEK{}, ks, cache)
	if err != nil {
		t.Fatalf("prepareStorageNonceEpoch: %v", err)
	}
	// Epoch must be bumped from the on-disk 6 to 7.
	if epoch != 7 {
		t.Errorf("epoch = %d, want 7 (bumped from 6)", epoch)
	}
	// Keystore must be hydrated with both DEKs (active + historical).
	for _, id := range []uint32{3, 4} {
		if !ks.Has(id) {
			t.Errorf("keystore missing DEK id=%d after hydration", id)
		}
	}
	// Cache must reflect the active storage DEK and the cutover gate.
	if id, ok := cache.ActiveStorageKeyID(); !ok || id != 3 {
		t.Errorf("cache ActiveStorageKeyID = (%d, %v), want (3, true)", id, ok)
	}
	if !cache.StorageEnvelopeActive() {
		t.Error("cache StorageEnvelopeActive = false, want true (sidecar has it set)")
	}
	// The bump must be durable for the next process load.
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Keys["3"].LocalEpoch != 7 {
		t.Errorf("persisted epoch = %d, want 7", sc.Keys["3"].LocalEpoch)
	}
}

func TestBuildEncryptionWriteWiring_DisabledOrUnconfigured_Cleartext(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		enabled     bool
		sidecarPath string
		kek         encryption.KEKUnwrapper
	}{
		{name: "flag_off", enabled: false, sidecarPath: "x.json", kek: wiringFakeKEK{}},
		{name: "no_kek", enabled: true, sidecarPath: "x.json", kek: nil},
		{name: "no_sidecar_path", enabled: true, sidecarPath: "", kek: wiringFakeKEK{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w, err := buildEncryptionWriteWiring(tc.enabled, "n1", tc.sidecarPath, tc.kek, encryption.NewKeystore())
			if err != nil {
				t.Fatalf("buildEncryptionWriteWiring: %v", err)
			}
			if w.cache == nil {
				t.Error("cache is nil; must be non-nil on every return path")
			}
			if w.cipher != nil || w.nonceFactory != nil {
				t.Errorf("cipher/nonceFactory should be nil when encryption is off/unconfigured")
			}
			if opts := w.pebbleOptions(); opts != nil {
				t.Errorf("pebbleOptions = %d, want nil (cleartext)", len(opts))
			}
		})
	}
}

func TestBuildEncryptionWriteWiring_ActiveDEK_WiresCipher(t *testing.T) {
	t.Parallel()
	path := writeActiveStorageSidecar(t, 2)
	w, err := buildEncryptionWriteWiring(true, "n1", path, wiringFakeKEK{}, encryption.NewKeystore())
	if err != nil {
		t.Fatalf("buildEncryptionWriteWiring: %v", err)
	}
	if w.cipher == nil || w.nonceFactory == nil {
		t.Fatal("cipher/nonceFactory must be wired when encryption is enabled with an active DEK")
	}
	// WithEncryption + WithStorageEnvelopeGate + WithStorageRegistrationGate
	// (Stage 7a-2 added the third).
	if opts := w.pebbleOptions(); len(opts) != 3 {
		t.Errorf("pebbleOptions = %d, want 3", len(opts))
	}
	// Cache must reflect the on-disk active DEK + cutover gate.
	if id, ok := w.cache.ActiveStorageKeyID(); !ok || id != 3 {
		t.Errorf("cache ActiveStorageKeyID = (%d, %v), want (3, true)", id, ok)
	}
}

func TestPrepareStorageNonceEpoch_SaturatedEpoch_RefusesWithExhausted(t *testing.T) {
	t.Parallel()
	// A storage DEK already at the 0xFFFF ceiling must refuse the
	// bump (the ErrLocalEpochExhausted log + error path), so wiring
	// fails closed rather than wrapping the epoch to 0.
	path := writeActiveStorageSidecar(t, 0xFFFF)
	_, err := prepareStorageNonceEpoch(path, wiringFakeKEK{}, encryption.NewKeystore(), encryption.NewStateCache())
	if err == nil {
		t.Fatal("expected error from saturated local_epoch, got nil")
	}
	if !errors.Is(err, encryption.ErrLocalEpochExhausted) {
		t.Errorf("error not marked ErrLocalEpochExhausted: %v", err)
	}
}

// TestNoteRaftEnvelopeCutoverStartup_NoSidecarPath pins the
// degraded-input behaviour: an empty sidecar path returns without
// any side effect (no log, no panic). This is the
// encryption-disabled startup case where no sidecar is configured.
func TestNoteRaftEnvelopeCutoverStartup_NoSidecarPath(t *testing.T) {
	t.Parallel()
	noteRaftEnvelopeCutoverStartup("")
}

// TestNoteRaftEnvelopeCutoverStartup_MissingSidecar pins the
// silent-on-missing-file behaviour: the Stage 6C-1 startup guard
// is the authoritative refuser of malformed sidecars; this helper
// only cares about a present sidecar with a non-zero cutover
// index, and silently returns on any ReadSidecar error.
func TestNoteRaftEnvelopeCutoverStartup_MissingSidecar(t *testing.T) {
	t.Parallel()
	noteRaftEnvelopeCutoverStartup(t.TempDir() + "/absent.json")
}

// TestNoteRaftEnvelopeCutoverStartup_ZeroCutoverIsSilent pins the
// Stage 3 default: a sidecar with RaftEnvelopeCutoverIndex == 0
// must not log — that is the pre-cutover state, which is
// universal until Stage 6E-2f flips the gate.
func TestNoteRaftEnvelopeCutoverStartup_ZeroCutoverIsSilent(t *testing.T) {
	t.Parallel()
	path := writeActiveStorageSidecar(t, 0) // RaftEnvelopeCutoverIndex defaults to 0
	noteRaftEnvelopeCutoverStartup(path)
}

// TestNoteRaftEnvelopeCutoverStartup_NonZeroCutoverLogs exercises
// the "active cutover but no wrap closure" branch end-to-end:
// write a sidecar with RaftEnvelopeCutoverIndex != 0, invoke the
// hook, and confirm it does not panic / does not mutate the
// sidecar. The log line itself is observable only via slog
// handlers in production; the test exists primarily so a future
// refactor that removes the read path fails compile or trips a
// regression in this asserted-no-panic branch.
func TestNoteRaftEnvelopeCutoverStartup_NonZeroCutoverLogs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := dir + "/keys.json"
	sc := &encryption.Sidecar{
		Version:                  encryption.SidecarVersion,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: 12345,
		Active:                   encryption.ActiveKeys{Storage: 3, Raft: 4},
		Keys: map[string]encryption.SidecarKey{
			"3": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage-wrapped-bytes")},
			"4": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft-wrapped-bytes-diff")},
		},
	}
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	noteRaftEnvelopeCutoverStartup(path)
	// Sidecar must be unchanged on disk — the hook is read-only.
	got, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar after hook: %v", err)
	}
	if got.RaftEnvelopeCutoverIndex != 12345 {
		t.Errorf("post-hook RaftEnvelopeCutoverIndex = %d, want 12345 (hook must be read-only)", got.RaftEnvelopeCutoverIndex)
	}
}
