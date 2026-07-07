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

func TestPrepareRaftNonceEpoch_ActiveDEK_HydratesAndBumps(t *testing.T) {
	t.Parallel()
	path := writeActiveStorageSidecar(t, 2)
	ks := encryption.NewKeystore()
	epoch, err := prepareRaftNonceEpoch(path, wiringFakeKEK{}, ks)
	if err != nil {
		t.Fatalf("prepareRaftNonceEpoch: %v", err)
	}
	if epoch != 1 {
		t.Errorf("raft epoch = %d, want 1 (bumped from 0)", epoch)
	}
	for _, id := range []uint32{3, 4} {
		if !ks.Has(id) {
			t.Errorf("keystore missing DEK id=%d after raft hydration", id)
		}
	}
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Keys["4"].LocalEpoch != 1 {
		t.Errorf("persisted raft epoch = %d, want 1", sc.Keys["4"].LocalEpoch)
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
			if w.cipher != nil || w.nonceFactory != nil || w.raftNonceFactory != nil {
				t.Errorf("cipher/nonce factories should be nil when encryption is off/unconfigured")
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
	if w.cipher == nil || w.nonceFactory == nil || w.raftNonceFactory == nil {
		t.Fatal("cipher/storage nonce/raft nonce factories must be wired when encryption is enabled with active DEKs")
	}
	if w.epoch != 3 {
		t.Errorf("storage epoch = %d, want 3 (bumped from 2)", w.epoch)
	}
	if w.raftEpoch != 1 {
		t.Errorf("raft epoch = %d, want 1 (bumped from 0)", w.raftEpoch)
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
