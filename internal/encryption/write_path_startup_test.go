package encryption_test

import (
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

// validStorageRaftSidecar writes a consistent two-DEK sidecar
// (storage id=7, raft id=8) to a temp path and returns the path. The
// storage DEK's local_epoch is set to storageEpoch; the raft DEK's
// epoch is fixed at 0 (this PR bumps only the storage write path).
// The wrapped bytes are distinct so fakeKEK derives distinct DEKs.
func validStorageRaftSidecar(t *testing.T, storageEpoch uint16) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/keys.json"
	sc := &encryption.Sidecar{
		Version: encryption.SidecarVersion,
		Active:  encryption.ActiveKeys{Storage: 7, Raft: 8},
		Keys: map[string]encryption.SidecarKey{
			"7": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("storage-wrapped-bytes"), LocalEpoch: storageEpoch},
			"8": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("raft-wrapped-bytes-diff"), LocalEpoch: 0},
		},
	}
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

func TestHydrateKeystoreFromSidecar_InstallsAllDEKs(t *testing.T) {
	t.Parallel()
	path := validStorageRaftSidecar(t, 0)
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	ks := encryption.NewKeystore()
	if err := encryption.HydrateKeystoreFromSidecar(ks, &fakeKEK{}, sc); err != nil {
		t.Fatalf("HydrateKeystoreFromSidecar: %v", err)
	}
	// Both the active storage DEK and the (historical-read) raft DEK
	// must be present so the cipher can decrypt every unretired
	// envelope after a restart.
	for _, id := range []uint32{7, 8} {
		if !ks.Has(id) {
			t.Errorf("keystore missing DEK id=%d after hydration", id)
		}
	}
}

func TestHydrateKeystoreFromSidecar_Idempotent(t *testing.T) {
	t.Parallel()
	path := validStorageRaftSidecar(t, 0)
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	ks := encryption.NewKeystore()
	kek := &fakeKEK{}
	if err := encryption.HydrateKeystoreFromSidecar(ks, kek, sc); err != nil {
		t.Fatalf("first hydration: %v", err)
	}
	// Second hydration over the already-populated keystore must be a
	// no-op (byte-identical DEKs) rather than an ErrKeyConflict.
	if err := encryption.HydrateKeystoreFromSidecar(ks, kek, sc); err != nil {
		t.Fatalf("second hydration (idempotent): %v", err)
	}
}

func TestHydrateKeystoreFromSidecar_KEKFailurePropagates(t *testing.T) {
	t.Parallel()
	path := validStorageRaftSidecar(t, 0)
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	boom := errors.New("kek unwrap boom")
	ks := encryption.NewKeystore()
	err = encryption.HydrateKeystoreFromSidecar(ks, &fakeKEK{failOn: []byte("storage-wrapped-bytes"), failErr: boom}, sc)
	if err == nil {
		t.Fatal("expected hydration error from KEK failure, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("error does not wrap KEK failure: %v", err)
	}
}

func TestHydrateKeystoreFromSidecar_NilArgs(t *testing.T) {
	t.Parallel()
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion}
	if err := encryption.HydrateKeystoreFromSidecar(nil, &fakeKEK{}, sc); err == nil {
		t.Error("nil keystore: expected error, got nil")
	}
	if err := encryption.HydrateKeystoreFromSidecar(encryption.NewKeystore(), nil, sc); err == nil {
		t.Error("nil kek: expected error, got nil")
	}
	if err := encryption.HydrateKeystoreFromSidecar(encryption.NewKeystore(), &fakeKEK{}, nil); err == nil {
		t.Error("nil sidecar: expected error, got nil")
	}
}

func TestBumpLocalEpoch_IncrementsAndPersists(t *testing.T) {
	t.Parallel()
	path := validStorageRaftSidecar(t, 4)
	got, err := encryption.BumpLocalEpoch(path, 7)
	if err != nil {
		t.Fatalf("BumpLocalEpoch: %v", err)
	}
	if got != 5 {
		t.Errorf("returned epoch = %d, want 5", got)
	}
	// The bump must be durable: re-reading the sidecar shows the
	// advanced epoch (so the next process load bumps from 5, not 4).
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Keys["7"].LocalEpoch != 5 {
		t.Errorf("persisted storage epoch = %d, want 5", sc.Keys["7"].LocalEpoch)
	}
	// The raft DEK's epoch must be untouched — this PR bumps only the
	// storage write path.
	if sc.Keys["8"].LocalEpoch != 0 {
		t.Errorf("raft epoch = %d, want 0 (bump must not touch raft)", sc.Keys["8"].LocalEpoch)
	}
}

func TestBumpLocalEpoch_RefusesAtSaturation(t *testing.T) {
	t.Parallel()
	path := validStorageRaftSidecar(t, 0xFFFF)
	_, err := encryption.BumpLocalEpoch(path, 7)
	if err == nil {
		t.Fatal("expected ErrLocalEpochExhausted at 0xFFFF, got nil")
	}
	if !errors.Is(err, encryption.ErrLocalEpochExhausted) {
		t.Errorf("error not marked ErrLocalEpochExhausted: %v", err)
	}
	// The sidecar must NOT have been mutated by the refused bump.
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Keys["7"].LocalEpoch != 0xFFFF {
		t.Errorf("epoch mutated despite refusal: %d", sc.Keys["7"].LocalEpoch)
	}
}

func TestBumpLocalEpoch_RejectsUnknownDEKAndReserved(t *testing.T) {
	t.Parallel()
	path := validStorageRaftSidecar(t, 0)
	if _, err := encryption.BumpLocalEpoch(path, 99); err == nil {
		t.Error("unknown dek_id: expected error, got nil")
	}
	if _, err := encryption.BumpLocalEpoch(path, 0); err == nil {
		t.Error("reserved dek_id 0: expected error, got nil")
	}
	if _, err := encryption.BumpLocalEpoch("", 7); err == nil {
		t.Error("empty path: expected error, got nil")
	}
}
