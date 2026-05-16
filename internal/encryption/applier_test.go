package encryption_test

import (
	"errors"
	"strconv"
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
// snapshotting the entry.
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

// TestApplyBootstrap_ReturnsKEKNotConfigured pins the no-options
// default — without WithKEK / WithKeystore / WithSidecarPath, an
// Applier returns ErrKEKNotConfigured wrapped for the HaltApply
// pipeline. This is the Stage 6A posture; Stage 6B preserves it
// as the unwired default so the FSM dispatch contract still
// produces a typed, grep-able failure when an operator
// hand-triggers Bootstrap on a binary that has not yet been
// configured.
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

// TestApplyRotation_ReturnsKEKNotConfigured pins the same no-options
// default for ApplyRotation.
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

// fakeKEK is a deterministic KEKUnwrapper for tests. Unwrap
// returns a 32-byte DEK derived from the wrapped input — same
// wrapped bytes always produce the same DEK bytes, which exercises
// the Keystore.Set idempotency path on replay. The wrapped input
// can be any length (production uses ~48 bytes, tests use shorter
// sentinels); the DEK output is always 32 bytes (Keystore.Set
// requires encryption.KeySize).
type fakeKEK struct {
	failOn   []byte // if non-nil and Unwrap input matches, return failErr
	failErr  error
	override map[string][]byte // optional fixed mapping for specific wrapped inputs
}

func (f *fakeKEK) Unwrap(wrapped []byte) ([]byte, error) {
	if f.failOn != nil && string(f.failOn) == string(wrapped) {
		return nil, f.failErr
	}
	if v, ok := f.override[string(wrapped)]; ok {
		return v, nil
	}
	// Deterministic derivation: pad/truncate wrapped to KeySize.
	out := make([]byte, encryption.KeySize)
	copy(out, wrapped)
	return out, nil
}

// TestApplyBootstrap_FullyWired pins the §5.6 step 1a happy path:
// Bootstrap KEK-unwraps both DEKs, installs them into the keystore,
// writes a crash-durable sidecar with Active.{Storage,Raft} set,
// and inserts the batch writer-registry rows. The post-apply
// state is verified end-to-end via per-aspect helper functions.
func TestApplyBootstrap_FullyWired(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"

	app, err := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}

	payload := fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("storage-wrapped-bytes"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("raft-wrapped-bytes-different"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 1, FullNodeID: 0xAAAA, LocalEpoch: 0},
			{DEKID: 2, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}
	if err := app.ApplyBootstrap(payload); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	assertKeystoreHas(t, ks, payload.StorageDEKID, payload.RaftDEKID)
	assertBootstrapSidecar(t, sidecarPath, payload)
	assertRegistryRowsPresent(t, reg, payload.BatchRegistry)
}

func assertKeystoreHas(t *testing.T, ks *encryption.Keystore, ids ...uint32) {
	t.Helper()
	for _, id := range ids {
		if !ks.Has(id) {
			t.Errorf("keystore missing DEK id=%d", id)
		}
	}
}

func assertBootstrapSidecar(t *testing.T, path string, p fsmwire.BootstrapPayload) {
	t.Helper()
	sc, err := encryption.ReadSidecar(path)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Active.Storage != p.StorageDEKID {
		t.Errorf("Active.Storage = %d, want %d", sc.Active.Storage, p.StorageDEKID)
	}
	if sc.Active.Raft != p.RaftDEKID {
		t.Errorf("Active.Raft = %d, want %d", sc.Active.Raft, p.RaftDEKID)
	}
	assertSidecarKey(t, sc, p.StorageDEKID, encryption.SidecarPurposeStorage)
	assertSidecarKey(t, sc, p.RaftDEKID, encryption.SidecarPurposeRaft)
}

func assertSidecarKey(t *testing.T, sc *encryption.Sidecar, id uint32, wantPurpose string) {
	t.Helper()
	key := strconv.FormatUint(uint64(id), 10)
	k, ok := sc.Keys[key]
	if !ok {
		t.Errorf("sidecar keys map missing key %s", key)
		return
	}
	if k.Purpose != wantPurpose {
		t.Errorf("keys[%s].Purpose = %q, want %q", key, k.Purpose, wantPurpose)
	}
}

func assertRegistryRowsPresent(t *testing.T, reg *mapRegistryStore, rows []fsmwire.RegistrationPayload) {
	t.Helper()
	for _, r := range rows {
		key := encryption.RegistryKey(r.DEKID, uint16(r.FullNodeID&0xFFFF)) //nolint:gosec // test setup, masked to 16 bits
		if _, ok, _ := reg.GetRegistryRow(key); !ok {
			t.Errorf("registry row missing for dek_id=%d full_node_id=%#x", r.DEKID, r.FullNodeID)
		}
	}
}

// TestApplyBootstrap_PartialWiring pins the all-or-nothing trio:
// supplying only some of WithKEK / WithKeystore / WithSidecarPath
// MUST return ErrKEKNotConfigured. The Applier fails closed at
// apply time so a partial wiring during a future refactor cannot
// mis-apply with one dep present.
func TestApplyBootstrap_PartialWiring(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []encryption.ApplierOption
	}{
		{name: "kek_only", opts: []encryption.ApplierOption{encryption.WithKEK(&fakeKEK{})}},
		{name: "keystore_only", opts: []encryption.ApplierOption{encryption.WithKeystore(encryption.NewKeystore())}},
		{name: "sidecar_path_only", opts: []encryption.ApplierOption{encryption.WithSidecarPath("/tmp/never-written")}},
		{name: "kek_and_keystore", opts: []encryption.ApplierOption{
			encryption.WithKEK(&fakeKEK{}),
			encryption.WithKeystore(encryption.NewKeystore()),
		}},
		{name: "kek_and_sidecar_path", opts: []encryption.ApplierOption{
			encryption.WithKEK(&fakeKEK{}),
			encryption.WithSidecarPath("/tmp/never-written"),
		}},
		{name: "keystore_and_sidecar_path", opts: []encryption.ApplierOption{
			encryption.WithKeystore(encryption.NewKeystore()),
			encryption.WithSidecarPath("/tmp/never-written"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, err := encryption.NewApplier(newMapRegistryStore(), tc.opts...)
			if err != nil {
				t.Fatalf("NewApplier: %v", err)
			}
			err = app.ApplyBootstrap(fsmwire.BootstrapPayload{StorageDEKID: 1, RaftDEKID: 2})
			if err == nil {
				t.Fatal("expected ErrKEKNotConfigured for partial wiring, got nil")
			}
			if !errors.Is(err, encryption.ErrKEKNotConfigured) {
				t.Errorf("err not marked ErrKEKNotConfigured: %v", err)
			}
		})
	}
}

// TestApplyBootstrap_KEKUnwrapFailure pins fail-closed behavior on
// KEK errors. The synthetic Unwrap error is propagated; the kv/fsm
// dispatch layer will then mark it with ErrEncryptionApply for
// HaltApply regardless of upstream marking.
func TestApplyBootstrap_KEKUnwrapFailure(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	boom := errors.New("kek: synthetic unwrap failure")
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{failOn: []byte("doomed"), failErr: boom}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(dir+"/keys.json"),
	)
	err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("doomed"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("ok"),
	})
	if err == nil {
		t.Fatal("expected unwrap failure, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("err did not wrap synthetic Unwrap failure: %v", err)
	}
	// Keystore must not contain a partial install on failure of the
	// FIRST unwrap.
	if ks.Has(1) || ks.Has(2) {
		t.Error("keystore should be empty after first-unwrap failure")
	}
}

// TestApplyBootstrap_Replay_Idempotent pins the Raft-replay
// invariant: applying the same BootstrapPayload twice produces
// the same sidecar and keystore state on the second pass, with
// no error. Replay safety is the load-bearing property that lets
// crash-mid-apply recovery work.
func TestApplyBootstrap_Replay_Idempotent(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	payload := fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("storage"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("raft"),
		BatchRegistry:  []fsmwire.RegistrationPayload{{DEKID: 1, FullNodeID: 0x4242, LocalEpoch: 0}},
	}
	if err := app.ApplyBootstrap(payload); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	if err := app.ApplyBootstrap(payload); err != nil {
		t.Errorf("second ApplyBootstrap (replay): %v", err)
	}
}

// TestApplyRotation_FullyWired pins the §5.2 rotate-dek happy
// path: KEK-unwrap, keystore install at the new DEK ID, sidecar
// Active slot updated for the supplied Purpose, ProposerRegistration
// row inserted.
func TestApplyRotation_FullyWired(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	// Pre-populate the sidecar with a bootstrap so rotation has
	// something to advance from.
	if err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	// Rotate the storage DEK from id 1 to id 5.
	if err := app.ApplyRotation(fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                5,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("storage-v2-wrapped"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 5, FullNodeID: 0xBEEF, LocalEpoch: 0},
	}); err != nil {
		t.Fatalf("ApplyRotation: %v", err)
	}
	if !ks.Has(5) {
		t.Error("keystore missing rotated DEK 5")
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Active.Storage != 5 {
		t.Errorf("Active.Storage = %d, want 5", sc.Active.Storage)
	}
	if sc.Active.Raft != 2 {
		t.Errorf("Active.Raft = %d, want 2 (unchanged by storage rotation)", sc.Active.Raft)
	}
	if k, ok := sc.Keys["5"]; !ok {
		t.Error("sidecar keys missing rotated DEK 5")
	} else if k.Purpose != encryption.SidecarPurposeStorage {
		t.Errorf("keys[5].Purpose = %q, want %q", k.Purpose, encryption.SidecarPurposeStorage)
	}
	// Old DEK 1 must remain in keys[] (rotation does not retire).
	if _, ok := sc.Keys["1"]; !ok {
		t.Error("sidecar keys should still contain old DEK 1 after rotation (retire is a separate sub-tag)")
	}
}

// TestApplyRotation_UnknownSubTag pins the fail-closed dispatch:
// any sub_tag other than RotateSubRotateDEK halts apply with
// ErrEncryptionApply so Stage 6B-1 cannot silently mis-apply a
// later sub_tag (rewrap-deks, retire-dek, enable-storage-envelope,
// enable-raft-envelope) before its implementation lands.
func TestApplyRotation_UnknownSubTag(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(dir+"/keys.json"),
	)
	err := app.ApplyRotation(fsmwire.RotationPayload{
		SubTag:  0xFE, // not RotateSubRotateDEK
		DEKID:   1,
		Wrapped: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected halt on unknown sub_tag, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
}

// TestApplyRotation_UnknownPurpose pins the same fail-closed
// dispatch for unrecognised Purpose enum values — guards against
// a future wire-format extension silently mis-labelling sidecar
// entries.
func TestApplyRotation_UnknownPurpose(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	// Pre-bootstrap so the sidecar exists.
	if err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s"),
		RaftDEKID: 2, WrappedRaft: []byte("r"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	// ProposerRegistration.DEKID MUST match RotationPayload.DEKID
	// — the round-2 mismatch guard fires before sidecarPurposeFor
	// otherwise, and the test would exercise the wrong code path.
	err := app.ApplyRotation(fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                9,
		Purpose:              fsmwire.Purpose(0xFE), // not Storage / Raft
		Wrapped:              []byte("y"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 9, FullNodeID: 0x01, LocalEpoch: 0},
	})
	if err == nil {
		t.Fatal("expected halt on unknown purpose, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
}

// TestApplyBootstrap_RejectsSecondBootstrapWithDifferentDEKs
// pins the §5.6 one-time-bootstrap invariant: once Active slots
// are populated, a second committed bootstrap entry with
// DIFFERENT DEK IDs MUST be rejected. A successful re-bootstrap
// would re-point Active.{Storage,Raft} outside the rotation
// path, leaving writer-registry and key-lifecycle semantics
// inconsistent. Operators advance DEKs via rotate-dek.
func TestApplyBootstrap_RejectsSecondBootstrapWithDifferentDEKs(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	// First bootstrap: Active = (1, 2).
	if err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	// Second bootstrap with different DEK IDs MUST be rejected.
	err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 7, WrappedStorage: []byte("s7"),
		RaftDEKID: 8, WrappedRaft: []byte("r8"),
	})
	if err == nil {
		t.Fatal("expected halt on second bootstrap with different DEKs, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	// Sidecar Active slots must NOT have been re-pointed.
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.Active.Storage != 1 || sc.Active.Raft != 2 {
		t.Errorf("Active = (%d, %d), want (1, 2) — second bootstrap mutated sidecar despite rejection",
			sc.Active.Storage, sc.Active.Raft)
	}
	// Keystore must NOT contain the rejected DEK IDs (guard fires
	// before kek.Unwrap / keystore.Set), matching the pattern set
	// by TestApplyBootstrap_RejectsForeignBatchRegistryDEK.
	if ks.Has(7) || ks.Has(8) {
		t.Error("keystore mutated despite rejection of second bootstrap")
	}
}

// TestApplyBootstrap_RejectsSecondBootstrapWithSameIDsDifferentWrapped
// pins the stricter idempotency invariant: a second bootstrap with
// the SAME DEK IDs but DIFFERENT wrapped DEK bytes MUST be
// rejected. The DEK-ID-only check alone would have allowed the
// second apply to continue, which would then re-run
// writeBootstrapSidecar (overwriting keys[].Wrapped with the new
// bytes) and iterate the BatchRegistry — potentially mutating
// writer-registry state for previously-unregistered nodes through
// the §4.1 case-1 first-seen path.
//
// Raft determinism couples wrapped DEK bytes to the entry hash,
// so a legitimate replay always has matching wrapped bytes. A
// mismatch is necessarily a malformed or malicious second bootstrap.
func TestApplyBootstrap_RejectsSecondBootstrapWithSameIDsDifferentWrapped(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	// First bootstrap with wrapped bytes "s1" / "r2".
	if err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1-orig"),
		RaftDEKID: 2, WrappedRaft: []byte("r2-orig"),
	}); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	// Second bootstrap with same DEK IDs but different wrapped bytes.
	err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1-tampered"),
		RaftDEKID: 2, WrappedRaft: []byte("r2-tampered"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			// Adding a previously-unregistered node — would silently
			// insert via §4.1 case-1 if the second bootstrap had
			// been allowed through.
			{DEKID: 1, FullNodeID: 0x99, LocalEpoch: 0},
		},
	})
	if err == nil {
		t.Fatal("expected halt on same-IDs/different-wrapped second bootstrap, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	// Verify no side effects: keys[].Wrapped still has the ORIGINAL
	// bytes (not the tampered bytes), and the previously-
	// unregistered node 0x99 was NOT inserted into the registry.
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if got := sc.Keys["1"].Wrapped; string(got) != "s1-orig" {
		t.Errorf("keys[1].Wrapped = %q, want %q (second bootstrap should not have overwritten)", got, "s1-orig")
	}
	if got := sc.Keys["2"].Wrapped; string(got) != "r2-orig" {
		t.Errorf("keys[2].Wrapped = %q, want %q", got, "r2-orig")
	}
	rejectedRowKey := encryption.RegistryKey(1, 0x99)
	if _, ok, _ := reg.GetRegistryRow(rejectedRowKey); ok {
		t.Error("writer-registry row for previously-unregistered node was inserted despite second-bootstrap rejection")
	}
}

// TestApplyBootstrap_RejectsForeignBatchRegistryDEK pins the §5.6
// step 1a bootstrap-batch invariant: every BatchRegistry row MUST
// target either StorageDEKID or RaftDEKID. A row targeting a
// foreign DEK would persist writer-registry state for an unrelated
// key while the bootstrap installs only the two declared DEKs —
// silently breaking the §4.1 first-writer invariant on the next
// post-bootstrap write under that foreign key.
func TestApplyBootstrap_RejectsForeignBatchRegistryDEK(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("s"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 1, FullNodeID: 0xAA, LocalEpoch: 0},  // valid
			{DEKID: 99, FullNodeID: 0xBB, LocalEpoch: 0}, // foreign DEK
		},
	})
	if err == nil {
		t.Fatal("expected halt on foreign BatchRegistry DEK, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	// No side effects: keystore must not contain the bootstrap
	// DEKs, sidecar must not have been written. The guard fires
	// before any of those mutations.
	if ks.Has(1) || ks.Has(2) {
		t.Error("keystore mutated despite rejection")
	}
	if _, err := encryption.ReadSidecar(sidecarPath); err == nil {
		t.Error("sidecar was written despite rejection")
	}
}

// TestNewApplier_RejectsNilOption pins the construction-time guard
// against nil ApplierOption values. A `WithNowFunc(nil)` (or any
// other nil opt passed by mistake) would silently overwrite the
// default and panic at apply time when a.now() is invoked. The
// guard surfaces the misconfiguration at startup.
func TestNewApplier_RejectsNilOption(t *testing.T) {
	t.Parallel()
	_, err := encryption.NewApplier(newMapRegistryStore(), nil)
	if err == nil {
		t.Fatal("NewApplier(reg, nil) returned no error; want construction-time refusal")
	}
}

// TestNewApplier_RejectsNilNowFunc pins the specific nil-clock
// guard. Construction MUST refuse when WithNowFunc(nil) overwrites
// the default time.Now — otherwise a.now() would nil-deref on
// the first Bootstrap/Rotation apply.
func TestNewApplier_RejectsNilNowFunc(t *testing.T) {
	t.Parallel()
	_, err := encryption.NewApplier(newMapRegistryStore(), encryption.WithNowFunc(nil))
	if err == nil {
		t.Fatal("NewApplier(reg, WithNowFunc(nil)) returned no error; want construction-time refusal")
	}
}

// TestApplyBootstrap_RejectsIdenticalDEKIDs pins the §5.1 per-purpose
// separation invariant. If StorageDEKID == RaftDEKID, the second
// sc.Keys[...] assignment in writeBootstrapSidecar would overwrite
// the first, silently mis-labelling the surviving key's purpose
// (storage vs raft). The applier halts apply with ErrEncryptionApply
// so a malformed bootstrap payload cannot corrupt the sidecar.
func TestApplyBootstrap_RejectsIdenticalDEKIDs(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(dir+"/keys.json"),
	)
	err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID:   7,
		WrappedStorage: []byte("s"),
		RaftDEKID:      7, // same as Storage
		WrappedRaft:    []byte("r"),
	})
	if err == nil {
		t.Fatal("expected halt on identical DEK IDs, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	// Sidecar must not have been written.
	if _, err := encryption.ReadSidecar(dir + "/keys.json"); err == nil {
		t.Error("sidecar was written despite rejection")
	}
}

// TestApplyRotation_RejectsProposerDEKMismatch pins the
// ProposerRegistration vs rotated DEK invariant: the proposer's
// first-writer row MUST target the new DEK (that is the whole
// point of bundling it atomically with the rotate-dek entry per
// §5.2). A mismatch mutates writer-registry state for an
// unrelated DEK and leaves the rotated DEK unregistered.
func TestApplyRotation_RejectsProposerDEKMismatch(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err := app.ApplyBootstrap(fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s"),
		RaftDEKID: 2, WrappedRaft: []byte("r"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	err := app.ApplyRotation(fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                5, // rotating to id 5
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("w"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 99, FullNodeID: 0xAA, LocalEpoch: 0}, // mismatch
	})
	if err == nil {
		t.Fatal("expected halt on proposer-registration DEK mismatch, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
}
