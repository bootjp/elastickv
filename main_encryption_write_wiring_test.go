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
	if opts := w.pebbleOptions(); len(opts) != 2 {
		t.Errorf("wired pebbleOptions = %d opts, want 2 (WithEncryption + WithStorageEnvelopeGate)", len(opts))
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
