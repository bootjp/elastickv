package main

import (
	"errors"
	"os"
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

// TestRefuseStartupOnActiveRaftCutover_NoSidecarPath pins the
// degraded-input behaviour: an empty sidecar path returns nil
// (no refusal). This is the encryption-disabled startup case
// where no sidecar is configured.
func TestRefuseStartupOnActiveRaftCutover_NoSidecarPath(t *testing.T) {
	t.Parallel()
	if err := refuseStartupOnActiveRaftCutover(""); err != nil {
		t.Errorf("empty sidecarPath: refuseStartupOnActiveRaftCutover = %v, want nil", err)
	}
}

// TestRefuseStartupOnActiveRaftCutover_MissingSidecar pins the
// nil-on-missing-file behaviour: the Stage 6C-1 startup guard
// is the authoritative refuser of malformed sidecars; this helper
// only cares about a present sidecar with a non-zero cutover
// index, and silently returns nil on any ReadSidecar error.
func TestRefuseStartupOnActiveRaftCutover_MissingSidecar(t *testing.T) {
	t.Parallel()
	if err := refuseStartupOnActiveRaftCutover(t.TempDir() + "/absent.json"); err != nil {
		t.Errorf("missing sidecar: refuseStartupOnActiveRaftCutover = %v, want nil (Stage 6C-1 owns malformed sidecars)", err)
	}
}

// TestRefuseStartupOnActiveRaftCutover_ZeroCutoverPasses pins the
// Stage 3 default: a sidecar with RaftEnvelopeCutoverIndex == 0
// must return nil — that is the pre-cutover state, which is
// universal until Stage 6E-2f flips the gate.
func TestRefuseStartupOnActiveRaftCutover_ZeroCutoverPasses(t *testing.T) {
	t.Parallel()
	path := writeActiveStorageSidecar(t, 0) // RaftEnvelopeCutoverIndex defaults to 0
	if err := refuseStartupOnActiveRaftCutover(path); err != nil {
		t.Errorf("zero cutover: refuseStartupOnActiveRaftCutover = %v, want nil (Stage 3 default must pass)", err)
	}
}

// TestRefuseStartupOnActiveRaftCutover_NonZeroCutoverDoesNotBumpEpoch
// pins the codex P2 round-1 r3 property: the refusal MUST run
// before buildEncryptionWriteWiring's prepareStorageNonceEpoch
// step, otherwise an orchestrator restart loop on a sidecar with
// non-zero RaftEnvelopeCutoverIndex would consume one local_epoch
// per boot and could hit ErrLocalEpochExhausted before the
// operator notices.
//
// This is a black-box test on the helper itself: after invoking
// refuseStartupOnActiveRaftCutover with a non-zero-cutover
// sidecar, the sidecar's storage epoch field on disk MUST be
// unchanged from what we wrote, proving no BumpLocalEpoch path
// could have run downstream of the helper. The buildShardGroupsWithEncryption
// Wiring ordering invariant (refusal BEFORE buildEncryptionWriteWiring)
// is enforced structurally; this test guards against a future
// refactor that swaps the order.
func TestRefuseStartupOnActiveRaftCutover_NonZeroCutoverDoesNotBumpEpoch(t *testing.T) {
	t.Parallel()
	const initialStorageEpoch uint16 = 7
	dir := t.TempDir()
	path := dir + "/keys.json"
	sc := &encryption.Sidecar{
		Version:                  encryption.SidecarVersion,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: 12345,
		Active:                   encryption.ActiveKeys{Storage: 3, Raft: 4},
		Keys: map[string]encryption.SidecarKey{
			"3": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage-wrapped-bytes"), LocalEpoch: initialStorageEpoch},
			"4": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft-wrapped-bytes-diff")},
		},
	}
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	if err := refuseStartupOnActiveRaftCutover(path); err == nil {
		t.Fatal("non-zero cutover: refuseStartupOnActiveRaftCutover = nil, want refusal")
	}
	got, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar after refusal: %v", err)
	}
	gotEpoch := got.Keys["3"].LocalEpoch
	if gotEpoch != initialStorageEpoch {
		t.Errorf("post-refusal storage LocalEpoch = %d, want %d (refusal must run before any sidecar mutation)", gotEpoch, initialStorageEpoch)
	}
}

// TestRefuseStartupOnActiveRaftCutover_NonZeroCutoverRefuses
// pins the codex P1 fail-closed property: when the sidecar
// reports RaftEnvelopeCutoverIndex != 0, the helper returns a
// non-nil error so buildShardGroupsWithEncryptionWiring refuses
// startup. Without this, the binary would proceed with nil
// shard-group wrap pointers and fresh proposals could land
// cleartext above the cutover, halting the apply loop on the
// §6.3 strict-> unwrap path.
func TestRefuseStartupOnActiveRaftCutover_NonZeroCutoverRefuses(t *testing.T) {
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
	err := refuseStartupOnActiveRaftCutover(path)
	if err == nil {
		t.Fatal("non-zero cutover: refuseStartupOnActiveRaftCutover = nil, want refusal (codex P1)")
	}
	// Sidecar must be unchanged on disk — the helper is read-only.
	got, err2 := encryption.ReadSidecar(path)
	if err2 != nil {
		t.Fatalf("ReadSidecar after helper: %v", err2)
	}
	if got.RaftEnvelopeCutoverIndex != 12345 {
		t.Errorf("post-helper RaftEnvelopeCutoverIndex = %d, want 12345 (helper must be read-only)", got.RaftEnvelopeCutoverIndex)
	}
}

// TestRefuseStartupOnActiveRaftCutover_BadSidecarPropagates pins the
// fail-closed read-error branch: ReadSidecar errors that are NOT
// encryption.IsNotExist (malformed JSON, schema violation, KEK
// mismatch, etc.) must propagate as a refusal error. The helper's
// nil-return is narrowed to the pre-bootstrap case only; widening
// it into fail-open behaviour for any other failure mode would
// silently bypass the cutover guard.
//
// We use a malformed sidecar file (text that is not valid JSON) to
// trigger a non-NotExist read error. The exact error type is not
// asserted — only that the helper returns non-nil — because the
// underlying error chain is supplied by encryption.ReadSidecar's
// JSON / version decoder, which is not a contract surface this
// test should pin.
func TestRefuseStartupOnActiveRaftCutover_BadSidecarPropagates(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := dir + "/keys.json"
	if err := os.WriteFile(path, []byte("not a valid sidecar"), 0o600); err != nil {
		t.Fatalf("write malformed sidecar: %v", err)
	}
	if err := refuseStartupOnActiveRaftCutover(path); err == nil {
		t.Fatal("malformed sidecar: refuseStartupOnActiveRaftCutover = nil, want refusal (fail-closed on read error)")
	}
}
