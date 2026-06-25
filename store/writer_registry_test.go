package store_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/store"
)

// newRegistryTestStore opens a fresh Pebble-backed MVCCStore for
// the writer-registry tests. Cleanup is handled via t.Cleanup so
// each test gets an isolated db dir.
func newRegistryTestStore(t *testing.T) store.MVCCStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "fsm.db")
	s, err := store.NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestWriterRegistryFor_RoundTrip pins the production wiring of the
// §6.3 EncryptionApplier's writer-registry surface: a row written
// via the Pebble-backed adapter is read back byte-identical on a
// subsequent Get.
func TestWriterRegistryFor_RoundTrip(t *testing.T) {
	t.Parallel()
	s := newRegistryTestStore(t)
	reg, err := store.WriterRegistryFor(s)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}

	key := encryption.RegistryKey(7, 0x1234)
	value := encryption.EncodeRegistryValue(encryption.RegistryValue{
		FullNodeID:          0xDEAD_BEEF_CAFE_BABE,
		FirstSeenLocalEpoch: 5,
		LastSeenLocalEpoch:  9,
	})

	// Missing row reads as (nil, false, nil) — NOT an error.
	v, ok, err := reg.GetRegistryRow(key)
	if err != nil {
		t.Fatalf("GetRegistryRow on missing: %v", err)
	}
	if ok {
		t.Errorf("expected missing row, got value %q", v)
	}

	if err := reg.SetRegistryRow(key, value); err != nil {
		t.Fatalf("SetRegistryRow: %v", err)
	}
	got, ok, err := reg.GetRegistryRow(key)
	if err != nil {
		t.Fatalf("GetRegistryRow after Set: %v", err)
	}
	if !ok {
		t.Fatal("expected row present after Set")
	}
	if string(got) != string(value) {
		t.Errorf("round-trip mismatch: got %x, want %x", got, value)
	}
}

// TestWriterRegistryFor_Overwrite pins the idempotency / overwrite
// semantics: SetRegistryRow on an existing key replaces the value;
// no MVCC versioning, no append.
func TestWriterRegistryFor_Overwrite(t *testing.T) {
	t.Parallel()
	s := newRegistryTestStore(t)
	reg, err := store.WriterRegistryFor(s)
	if err != nil {
		t.Fatalf("WriterRegistryFor: %v", err)
	}

	key := encryption.RegistryKey(1, 0x42)
	v1 := encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 1, FirstSeenLocalEpoch: 1, LastSeenLocalEpoch: 1})
	v2 := encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 1, FirstSeenLocalEpoch: 1, LastSeenLocalEpoch: 9})

	if err := reg.SetRegistryRow(key, v1); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if err := reg.SetRegistryRow(key, v2); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	got, ok, err := reg.GetRegistryRow(key)
	if err != nil || !ok {
		t.Fatalf("GetRegistryRow: ok=%v err=%v", ok, err)
	}
	if string(got) != string(v2) {
		t.Errorf("overwrite mismatch: got %x, want %x", got, v2)
	}
}

// TestWriterRegistryFor_RejectsNonPebble pins the construction-time
// guard. We use a minimal MVCCStore stub that has no Pebble backing
// — passing it to WriterRegistryFor MUST surface
// ErrUnsupportedStoreForWriterRegistry at construction time so the
// startup wiring catches the misconfiguration before the FSM apply
// path can nil-deref.
func TestWriterRegistryFor_RejectsNonPebble(t *testing.T) {
	t.Parallel()
	_, err := store.WriterRegistryFor(noopMVCCStub{})
	if err == nil {
		t.Fatal("WriterRegistryFor(non-pebble) returned no error; want refusal")
	}
	if !errors.Is(err, store.ErrUnsupportedStoreForWriterRegistry) {
		t.Errorf("err not marked ErrUnsupportedStoreForWriterRegistry: %v", err)
	}
}

// noopMVCCStub is a do-nothing MVCCStore used only to verify the
// type-assertion refusal path. Every method panics — the test must
// not reach them, which is exactly the contract being verified
// (WriterRegistryFor returns ErrUnsupportedStoreForWriterRegistry
// before calling any method on the supplied store).
type noopMVCCStub struct{ store.MVCCStore }
