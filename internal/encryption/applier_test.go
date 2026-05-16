package encryption_test

import (
	"errors"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
)

// mapRegistryStore is a small in-memory WriterRegistryStore used by
// the applier tests. It is deliberately ordered by raw byte slice
// comparisons so tests that exercise key shape (e.g., uint16
// truncation collision) match production behaviour. The map is keyed
// by string(key) because []byte is not comparable.
type mapRegistryStore struct {
	rows  map[string][]byte
	setEr error
	getEr error
}

func newMapRegistryStore() *mapRegistryStore {
	return &mapRegistryStore{rows: map[string][]byte{}}
}

func (m *mapRegistryStore) GetRegistryRow(key []byte) ([]byte, bool, error) {
	if m.getEr != nil {
		return nil, false, m.getEr
	}
	v, ok := m.rows[string(key)]
	return v, ok, nil
}

func (m *mapRegistryStore) SetRegistryRow(key []byte, value []byte) error {
	if m.setEr != nil {
		return m.setEr
	}
	m.rows[string(key)] = append([]byte(nil), value...)
	return nil
}

// TestNewApplier_RejectsNilRegistry pins the construction-time guard
// — a nil registry would nil-deref deep in Raft apply, which is the
// hardest possible site to diagnose. Catching it at construction
// surfaces the wiring mistake at startup.
func TestNewApplier_RejectsNilRegistry(t *testing.T) {
	t.Parallel()
	app, err := encryption.NewApplier(nil)
	if err == nil {
		t.Fatalf("NewApplier(nil) returned no error; want construction-time refusal")
	}
	if app != nil {
		t.Errorf("NewApplier(nil) returned non-nil applier with error; want nil applier")
	}
}

// TestApplyRegistration_Case1_Insert pins §4.1 case 1: a first-seen
// (dek_id, uint16(node_id)) writes a fresh row with FirstSeen ==
// LastSeen == incoming local_epoch.
func TestApplyRegistration_Case1_Insert(t *testing.T) {
	t.Parallel()
	store := newMapRegistryStore()
	app, err := encryption.NewApplier(store)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	in := fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xDEAD_BEEF_CAFE_BABE, LocalEpoch: 5}
	if err := app.ApplyRegistration(in); err != nil {
		t.Fatalf("ApplyRegistration: %v", err)
	}
	key := encryption.RegistryKey(in.DEKID, uint16(in.FullNodeID&0xFFFF)) //nolint:gosec // test setup, masked to 16 bits
	raw, ok, err := store.GetRegistryRow(key)
	if err != nil {
		t.Fatalf("GetRegistryRow: %v", err)
	}
	if !ok {
		t.Fatal("expected row inserted, got missing")
	}
	got, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		t.Fatalf("DecodeRegistryValue: %v", err)
	}
	want := encryption.RegistryValue{
		FullNodeID:          in.FullNodeID,
		FirstSeenLocalEpoch: in.LocalEpoch,
		LastSeenLocalEpoch:  in.LocalEpoch,
	}
	if got != want {
		t.Errorf("inserted value = %+v, want %+v", got, want)
	}
}

// TestApplyRegistration_Case2_MonotonicAdvance pins §4.1 case 2:
// re-registering the same (dek_id, full_node_id) with a strictly
// greater local_epoch advances LastSeenLocalEpoch and preserves
// FirstSeenLocalEpoch.
func TestApplyRegistration_Case2_MonotonicAdvance(t *testing.T) {
	t.Parallel()
	store := newMapRegistryStore()
	app, _ := encryption.NewApplier(store)
	const dek = 9
	nodeID := uint64(0x0123_4567_89AB_CDEF)
	if err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: nodeID, LocalEpoch: 1}); err != nil {
		t.Fatalf("first ApplyRegistration: %v", err)
	}
	if err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: nodeID, LocalEpoch: 3}); err != nil {
		t.Fatalf("second ApplyRegistration: %v", err)
	}
	raw, _, _ := store.GetRegistryRow(encryption.RegistryKey(dek, uint16(nodeID&0xFFFF))) //nolint:gosec // test setup, masked to 16 bits
	got, _ := encryption.DecodeRegistryValue(raw)
	if got.FirstSeenLocalEpoch != 1 {
		t.Errorf("FirstSeenLocalEpoch = %d, want 1 (preserved across case 2 advance)", got.FirstSeenLocalEpoch)
	}
	if got.LastSeenLocalEpoch != 3 {
		t.Errorf("LastSeenLocalEpoch = %d, want 3 (advanced)", got.LastSeenLocalEpoch)
	}
}

// TestApplyRegistration_Case3_Rollback pins §4.1 case 3 (epoch
// rollback): re-registering with a strictly-smaller local_epoch
// under the same FullNodeID MUST halt apply with
// ErrEncryptionApply. Replaying a stale local_epoch under the
// same DEK would let a re-issued nonce collide with a
// previously-issued one, breaking AEAD confidentiality.
//
// Equal-epoch is NOT rollback — it is idempotent replay (Raft
// re-applies committed entries after restart until the latest
// FSM snapshot). The equal-epoch path is exercised by
// TestApplyRegistration_Case2_IdempotentReplay below.
func TestApplyRegistration_Case3_Rollback(t *testing.T) {
	t.Parallel()
	store := newMapRegistryStore()
	app, _ := encryption.NewApplier(store)
	const dek = 1
	nodeID := uint64(0xAAAA_BBBB_CCCC_DDDD)
	if err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: nodeID, LocalEpoch: 5}); err != nil {
		t.Fatalf("first ApplyRegistration: %v", err)
	}
	err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: nodeID, LocalEpoch: 4})
	if err == nil {
		t.Fatalf("expected rollback error, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
}

// TestApplyRegistration_Case2_IdempotentReplay pins the
// idempotent-replay path: a duplicate RegisterEncryptionWriter
// entry with the same (dek_id, full_node_id, local_epoch) MUST
// succeed with no error and no observable row change. Raft
// replays committed entries after restart until the latest FSM
// snapshot; rejecting equal epochs as rollback would pin a
// node in a halt-on-replay loop after any crash before
// snapshotting the entry (PR #765 round-2 codex P1).
func TestApplyRegistration_Case2_IdempotentReplay(t *testing.T) {
	t.Parallel()
	store := newMapRegistryStore()
	app, _ := encryption.NewApplier(store)
	const dek = 3
	nodeID := uint64(0xCAFE_BABE_DEAD_BEEF)
	first := fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: nodeID, LocalEpoch: 7}
	if err := app.ApplyRegistration(first); err != nil {
		t.Fatalf("first ApplyRegistration: %v", err)
	}
	// Snapshot row state after the first apply.
	keyForRow := encryption.RegistryKey(dek, uint16(nodeID&0xFFFF)) //nolint:gosec // test setup, masked to 16 bits
	before, _, _ := store.GetRegistryRow(keyForRow)
	// Replay the SAME entry — must be a no-op success.
	if err := app.ApplyRegistration(first); err != nil {
		t.Errorf("idempotent replay returned error: %v", err)
	}
	after, _, _ := store.GetRegistryRow(keyForRow)
	if string(before) != string(after) {
		t.Errorf("idempotent replay altered row: before=%x after=%x", before, after)
	}
}

// TestApplyRegistration_Case4_Uint16Collision pins §6.1's uniqueness
// invariant at apply time: two distinct full_node_ids whose lower
// 16 bits collide MUST halt apply. The startup ErrNodeIDCollision
// guard (6C) prevents this from being reachable in production; this
// is the per-node backstop.
func TestApplyRegistration_Case4_Uint16Collision(t *testing.T) {
	t.Parallel()
	store := newMapRegistryStore()
	app, _ := encryption.NewApplier(store)
	const dek = 2
	// Two full_node_ids that collide in the lower 16 bits (both
	// truncate to 0x1234). Declared as vars so uint16 truncation
	// is a runtime conversion, not a constant overflow.
	first := uint64(0x0000_0000_0000_1234)
	second := uint64(0xDEAD_BEEF_0000_1234)
	if uint16(first&0xFFFF) != uint16(second&0xFFFF) { //nolint:gosec // test setup, masked to 16 bits
		t.Fatalf("test setup wrong: first and second do not collide in uint16")
	}
	if err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: first, LocalEpoch: 1}); err != nil {
		t.Fatalf("first ApplyRegistration: %v", err)
	}
	err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: second, LocalEpoch: 1})
	if err == nil {
		t.Fatalf("expected collision error, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
}

// TestApplyRegistration_StoreErrorPropagates pins fail-closed behavior
// on transient storage faults: a Get or Set error from the registry
// store is wrapped and returned (not swallowed). The kv/fsm dispatch
// layer marks the result with ErrEncryptionApply so the engine's
// HaltApply seam fires regardless of whether the underlying error
// is marked.
func TestApplyRegistration_StoreErrorPropagates(t *testing.T) {
	t.Parallel()
	t.Run("get_error", func(t *testing.T) {
		store := newMapRegistryStore()
		boom := errors.New("pebble: synthetic get failure")
		store.getEr = boom
		app, _ := encryption.NewApplier(store)
		err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: 1, FullNodeID: 0x1234, LocalEpoch: 1})
		if err == nil {
			t.Fatalf("expected propagation of Get error, got nil")
		}
		if !errors.Is(err, boom) {
			t.Errorf("err did not wrap synthetic Get failure: %v", err)
		}
	})
	t.Run("set_error", func(t *testing.T) {
		store := newMapRegistryStore()
		boom := errors.New("pebble: synthetic set failure")
		store.setEr = boom
		app, _ := encryption.NewApplier(store)
		err := app.ApplyRegistration(fsmwire.RegistrationPayload{DEKID: 1, FullNodeID: 0x1234, LocalEpoch: 1})
		if err == nil {
			t.Fatalf("expected propagation of Set error, got nil")
		}
		if !errors.Is(err, boom) {
			t.Errorf("err did not wrap synthetic Set failure: %v", err)
		}
	})
}

// TestApplyBootstrap_ReturnsKEKNotConfigured pins the Stage 6A stub
// contract: until 6B threads a KEK unwrapper, ApplyBootstrap is the
// defense-in-depth marker returning ErrKEKNotConfigured (wrapped so
// the kv/fsm dispatch layer's ErrEncryptionApply marking still
// fires).
func TestApplyBootstrap_ReturnsKEKNotConfigured(t *testing.T) {
	t.Parallel()
	app, _ := encryption.NewApplier(newMapRegistryStore())
	err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("wrapped-storage"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("wrapped-raft"),
	})
	if err == nil {
		t.Fatalf("expected ErrKEKNotConfigured, got nil")
	}
	if !errors.Is(err, encryption.ErrKEKNotConfigured) {
		t.Errorf("err not marked ErrKEKNotConfigured: %v", err)
	}
}

// TestApplyRotation_ReturnsKEKNotConfigured pins the same Stage 6A
// stub contract for rotation.
func TestApplyRotation_ReturnsKEKNotConfigured(t *testing.T) {
	t.Parallel()
	app, _ := encryption.NewApplier(newMapRegistryStore())
	err := app.ApplyRotation(fsmwire.RotationPayload{
		SubTag:  0x01,
		DEKID:   1,
		Wrapped: []byte("wrapped"),
	})
	if err == nil {
		t.Fatalf("expected ErrKEKNotConfigured, got nil")
	}
	if !errors.Is(err, encryption.ErrKEKNotConfigured) {
		t.Errorf("err not marked ErrKEKNotConfigured: %v", err)
	}
}
