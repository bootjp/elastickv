package encryption_test

import (
	"errors"
	"strconv"
	"strings"
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
	err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
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
	err := app.ApplyRotation(0, fsmwire.RotationPayload{
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
	if err := app.ApplyBootstrap(0, payload); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	assertKeystoreHas(t, ks, payload.StorageDEKID, payload.RaftDEKID)
	assertBootstrapSidecar(t, sidecarPath, payload)
	assertRegistryRowsPresent(t, reg, payload.BatchRegistry)
}

// TestApplyBootstrap_PersistsRaftAppliedIndex pins the §9.1 guard
// invariant introduced by Stage 6C-2d: when raftengine threads a
// non-zero entry index through to ApplyBootstrap via the
// ApplyIndexAware seam, the applier writes that index into the
// sidecar's RaftAppliedIndex field in the SAME crash-durable
// WriteSidecar fsync that mutates the keys[] map.
//
// Without this, sidecar.RaftAppliedIndex stayed at 0 even after a
// successful Bootstrap commit; the next process start would see
// sidecar(0) << engine(>=K), scan (0, K], find the Bootstrap
// opcode at index K, and refuse boot with ErrSidecarBehindRaftLog
// — i.e. normal restart would be wrongly rejected. This test
// fails on any version that omits the raftIdx → sidecar wiring.
func TestApplyBootstrap_PersistsRaftAppliedIndex(t *testing.T) {
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
		},
	}
	const wantIdx uint64 = 184273
	if err := app.ApplyBootstrap(wantIdx, payload); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftAppliedIndex != wantIdx {
		t.Fatalf("sidecar.RaftAppliedIndex = %d, want %d (§9.1 guard would over-fire on next restart)",
			sc.RaftAppliedIndex, wantIdx)
	}
}

// TestApplyBootstrap_ZeroRaftIdxPreservesExisting pins the
// skip-on-zero policy of advanceRaftAppliedIndex: callers that did
// not opt into ApplyIndexAware (e.g. unit tests that exercise
// ApplyBootstrap directly without setting pendingApplyIdx, or
// engines that have not been updated to call SetApplyIndex) MUST
// NOT see a previously-recorded RaftAppliedIndex regress to 0.
// That would silently break the guard's freshness invariant after
// a future restart.
func TestApplyBootstrap_ZeroRaftIdxPreservesExisting(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"

	// Seed an existing sidecar with a non-zero RaftAppliedIndex so
	// the zero-arg call has something to potentially regress.
	seed := &encryption.Sidecar{
		Version:          encryption.SidecarVersion,
		RaftAppliedIndex: 9999,
		Active:           encryption.ActiveKeys{Storage: 1, Raft: 2},
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("s")},
			"2": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("r")},
		},
	}
	if err := encryption.WriteSidecar(sidecarPath, seed); err != nil {
		t.Fatalf("WriteSidecar seed: %v", err)
	}

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
		WrappedStorage: []byte("s"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 1, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}
	if err := app.ApplyBootstrap(0, payload); err != nil {
		t.Fatalf("ApplyBootstrap(raftIdx=0): %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftAppliedIndex != 9999 {
		t.Fatalf("zero-arg ApplyBootstrap regressed RaftAppliedIndex: %d, want preserved 9999",
			sc.RaftAppliedIndex)
	}
}

// TestApplyRotation_PersistsRaftAppliedIndex is the rotation
// counterpart of TestApplyBootstrap_PersistsRaftAppliedIndex.
// Same invariant: a non-zero raftIdx threaded through ApplyRotation
// must land in sidecar.RaftAppliedIndex inside the same fsync as
// the keys[] mutation.
func TestApplyRotation_PersistsRaftAppliedIndex(t *testing.T) {
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
	// Bootstrap first to land an initial sidecar with two keys
	// installed (rotation requires an existing sidecar — its
	// ReadSidecar returns an error otherwise).
	bp := fsmwire.BootstrapPayload{
		StorageDEKID:   1,
		WrappedStorage: []byte("s"),
		RaftDEKID:      2,
		WrappedRaft:    []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 1, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}
	if err := app.ApplyBootstrap(100, bp); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	const wantIdx uint64 = 200
	rp := fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubRotateDEK,
		Purpose: fsmwire.PurposeStorage,
		DEKID:   3,
		Wrapped: []byte("new-storage"),
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: 3, FullNodeID: 0xAAAA, LocalEpoch: 1,
		},
	}
	if err := app.ApplyRotation(wantIdx, rp); err != nil {
		t.Fatalf("ApplyRotation: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftAppliedIndex != wantIdx {
		t.Fatalf("sidecar.RaftAppliedIndex after rotation = %d, want %d",
			sc.RaftAppliedIndex, wantIdx)
	}
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
			err = app.ApplyBootstrap(0, fsmwire.BootstrapPayload{StorageDEKID: 1, RaftDEKID: 2})
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
	err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
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
	if err := app.ApplyBootstrap(0, payload); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	if err := app.ApplyBootstrap(0, payload); err != nil {
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
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	// Rotate the storage DEK from id 1 to id 5.
	if err := app.ApplyRotation(0, fsmwire.RotationPayload{
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

// TestApplyRotateDEK_LocalEpochAppliedToStorageKey pins Stage 7b'
// §3.1: with WithLocalEpoch installed, a PurposeStorage rotation
// writes Keys[newDEK].LocalEpoch = the installed value (the local
// node's pinned w.epoch) rather than the proposer's `0`. This is the
// per-node sidecar coordination that makes the §9.1 startup guard
// see a monotone advance on next restart instead of the brick
// scenario described in 7b' §1.
func TestApplyRotateDEK_LocalEpochAppliedToStorageKey(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	const pinnedEpoch uint16 = 7
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithLocalEpoch(pinnedEpoch),
	)
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	if err := app.ApplyRotation(0, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                5,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("storage-v2-wrapped"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 5, FullNodeID: 0xBEEF, LocalEpoch: 0},
	}); err != nil {
		t.Fatalf("ApplyRotation: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	got, ok := sc.Keys["5"]
	if !ok {
		t.Fatal("sidecar keys missing rotated DEK 5")
	}
	if got.LocalEpoch != pinnedEpoch {
		t.Errorf("Keys[5].LocalEpoch = %d, want %d (WithLocalEpoch's pinned value)", got.LocalEpoch, pinnedEpoch)
	}
}

// TestApplyRotateDEK_LocalEpoch_RaftRotationKeepsZero pins 7b' §3.1.1:
// the WithLocalEpoch value is the STORAGE write-path's pinned
// `w.epoch`. Raft DEK rotations have their own per-purpose epoch
// counter; cross-applying the storage epoch to a raft DEK's
// LocalEpoch would corrupt the raft counter. PurposeRaft rotations
// keep LocalEpoch: 0 when WithRaftLocalEpoch is not installed, even
// with WithLocalEpoch installed.
func TestApplyRotateDEK_LocalEpoch_RaftRotationKeepsZero(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithLocalEpoch(7), // storage epoch
	)
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	// Rotate the RAFT DEK from 2 to 6 — the storage WithLocalEpoch
	// value must NOT bleed into the new raft key's LocalEpoch.
	if err := app.ApplyRotation(0, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                6,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte("raft-v2-wrapped"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 6, FullNodeID: 0xBEEF, LocalEpoch: 0},
	}); err != nil {
		t.Fatalf("ApplyRotation: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	got, ok := sc.Keys["6"]
	if !ok {
		t.Fatal("sidecar keys missing rotated raft DEK 6")
	}
	if got.LocalEpoch != 0 {
		t.Errorf("Keys[6].LocalEpoch = %d, want 0 (raft rotation MUST NOT inherit storage write-path epoch — 7b' §3.1.1)", got.LocalEpoch)
	}
}

func TestApplyRotateDEK_RaftLocalEpochAppliedToRaftKey(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	const raftEpoch uint16 = 13
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithLocalEpoch(7),
		encryption.WithRaftLocalEpoch(raftEpoch),
	)
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	if err := app.ApplyRotation(0, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                6,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte("raft-v2-wrapped"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 6, FullNodeID: 0xBEEF, LocalEpoch: raftEpoch},
	}); err != nil {
		t.Fatalf("ApplyRotation: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	got, ok := sc.Keys["6"]
	if !ok {
		t.Fatal("sidecar keys missing rotated raft DEK 6")
	}
	if got.LocalEpoch != raftEpoch {
		t.Errorf("Keys[6].LocalEpoch = %d, want %d (WithRaftLocalEpoch's pinned value)", got.LocalEpoch, raftEpoch)
	}
}

// TestApplyRotateDEK_LocalEpoch_NoOptionFallsBackToZero pins 7b'
// §3.1.2 backward compatibility: an Applier constructed without
// WithLocalEpoch preserves today's Keys[newDEK].LocalEpoch=0 for
// PurposeStorage rotations so existing FSM-internal test harnesses
// (and pre-bootstrap process loads with w.epoch=0) keep working
// unchanged.
func TestApplyRotateDEK_LocalEpoch_NoOptionFallsBackToZero(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	// NO WithLocalEpoch option — a.localEpoch defaults to 0.
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	if err := app.ApplyRotation(0, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                5,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("storage-v2-wrapped"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 5, FullNodeID: 0xBEEF, LocalEpoch: 0},
	}); err != nil {
		t.Fatalf("ApplyRotation: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	got, ok := sc.Keys["5"]
	if !ok {
		t.Fatal("sidecar keys missing rotated DEK 5")
	}
	if got.LocalEpoch != 0 {
		t.Errorf("Keys[5].LocalEpoch = %d, want 0 (no WithLocalEpoch → falls back to pre-7b' behavior)", got.LocalEpoch)
	}
}

// TestApplyRotateDEK_LocalEpoch_AppliedPerApply pins 7b' §5 verification
// item 3: the static a.localEpoch field is read on EACH apply, so a
// second rotation observes the same value (not inadvertently reset
// or derived from mutable state). Two PurposeStorage rotations in
// sequence — both Keys[].LocalEpoch entries must equal the pinned
// value.
func TestApplyRotateDEK_LocalEpoch_AppliedPerApply(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	const pinnedEpoch uint16 = 11
	app, _ := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
		encryption.WithLocalEpoch(pinnedEpoch),
	)
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	for _, dek := range []uint32{5, 6} {
		if err := app.ApplyRotation(0, fsmwire.RotationPayload{
			SubTag:               fsmwire.RotateSubRotateDEK,
			DEKID:                dek,
			Purpose:              fsmwire.PurposeStorage,
			Wrapped:              []byte("storage-vN-wrapped"),
			ProposerRegistration: fsmwire.RegistrationPayload{DEKID: dek, FullNodeID: 0xBEEF, LocalEpoch: 0},
		}); err != nil {
			t.Fatalf("ApplyRotation dek=%d: %v", dek, err)
		}
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	for _, dek := range []string{"5", "6"} {
		got, ok := sc.Keys[dek]
		if !ok {
			t.Errorf("sidecar keys missing rotated DEK %s", dek)
			continue
		}
		if got.LocalEpoch != pinnedEpoch {
			t.Errorf("Keys[%s].LocalEpoch = %d, want %d (per-apply read of static field)", dek, got.LocalEpoch, pinnedEpoch)
		}
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
	err := app.ApplyRotation(0, fsmwire.RotationPayload{
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
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s"),
		RaftDEKID: 2, WrappedRaft: []byte("r"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	// ProposerRegistration.DEKID MUST match RotationPayload.DEKID
	// — the round-2 mismatch guard fires before sidecarPurposeFor
	// otherwise, and the test would exercise the wrong code path.
	err := app.ApplyRotation(0, fsmwire.RotationPayload{
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

// TestApplyRotation_EnableStorageEnvelope_FreshSuccess pins the
// Stage 6D-4 happy-path apply: after Bootstrap, a cutover entry
// with DEKID == Active.Storage, empty Wrapped, and
// Purpose == PurposeStorage flips StorageEnvelopeActive and records
// StorageEnvelopeCutoverIndex inside one sidecar fsync. The
// ProposerRegistration row is inserted via the standard §4.1 case
// dispatch so the proposer's first encrypted write under the
// now-active envelope is covered.
//
// Run with Wrapped both as `nil` and as `[]byte{}` to pin the
// length-based check on the applier side — `Wrapped == nil` is the
// in-Go construction, `[]byte{}` is what the wire decoder
// materialises for a `wrapped_len = 0` payload.
func TestApplyRotation_EnableStorageEnvelope_FreshSuccess(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		wrapped []byte
	}{
		{name: "nil_wrapped", wrapped: nil},
		{name: "empty_wrapped", wrapped: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runCutoverFreshSuccess(t, tc.wrapped)
		})
	}
}

func runCutoverFreshSuccess(t *testing.T, wrapped []byte) {
	t.Helper()
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
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 7, WrappedStorage: []byte("s"),
		RaftDEKID: 8, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	const cutoverIdx uint64 = 500
	if err := app.ApplyRotation(cutoverIdx, fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:   7, // matches sidecar.Active.Storage
		Purpose: fsmwire.PurposeStorage,
		Wrapped: wrapped,
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 1,
		},
	}); err != nil {
		t.Fatalf("ApplyRotation cutover: %v", err)
	}
	assertCutoverApplied(t, sidecarPath, cutoverIdx)
	assertProposerEpoch(t, reg, 7, 0xAAAA, 1)
}

// assertCutoverApplied verifies the fresh-success post-conditions:
// StorageEnvelopeActive flipped, StorageEnvelopeCutoverIndex set
// to raftIdx, RaftAppliedIndex advanced, Active.Storage unchanged.
func assertCutoverApplied(t *testing.T, sidecarPath string, cutoverIdx uint64) {
	t.Helper()
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if !sc.StorageEnvelopeActive {
		t.Error("StorageEnvelopeActive = false after cutover, want true")
	}
	if sc.StorageEnvelopeCutoverIndex != cutoverIdx {
		t.Errorf("StorageEnvelopeCutoverIndex = %d, want %d",
			sc.StorageEnvelopeCutoverIndex, cutoverIdx)
	}
	if sc.RaftAppliedIndex != cutoverIdx {
		t.Errorf("RaftAppliedIndex = %d, want %d", sc.RaftAppliedIndex, cutoverIdx)
	}
	// Active slot must NOT have been re-pointed — cutover does not
	// install a new DEK.
	if sc.Active.Storage != 7 {
		t.Errorf("Active.Storage = %d, want 7 (cutover should not re-point)", sc.Active.Storage)
	}
}

// assertProposerEpoch reads the writer-registry row for the
// (dekID, nodeID) pair and asserts the LastSeenLocalEpoch matches
// the cutover's LocalEpoch — proves the ProposerRegistration
// insert happened on the fresh-success path.
func assertProposerEpoch(t *testing.T, reg *mapRegistryStore, dekID uint32, nodeID uint16, want uint16) {
	t.Helper()
	key := encryption.RegistryKey(dekID, nodeID)
	raw, ok, err := reg.GetRegistryRow(key)
	if err != nil {
		t.Fatalf("GetRegistryRow: %v", err)
	}
	if !ok {
		t.Fatal("proposer registration row missing after cutover")
	}
	val, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		t.Fatalf("DecodeRegistryValue: %v", err)
	}
	if val.LastSeenLocalEpoch != want {
		t.Errorf("LastSeenLocalEpoch = %d, want %d", val.LastSeenLocalEpoch, want)
	}
}

// TestApplyRotation_EnableStorageEnvelope_RejectsNonStoragePurpose
// pins constraint #1 from §2.1 on the apply side: a malformed
// cutover with Purpose=PurposeRaft must halt apply rather than
// silently mis-flipping the storage-envelope flag based on a raft
// DEK reference.
func TestApplyRotation_EnableStorageEnvelope_RejectsNonStoragePurpose(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7,
		Purpose:              fsmwire.PurposeRaft, // wrong — cutover targets storage envelope
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 1},
	})
	if err == nil {
		t.Fatal("expected halt on non-storage purpose, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableStorageEnvelope_RejectsNonEmptyWrapped
// pins constraint #2 from §2.1: a cutover entry carrying any
// wrapped bytes is malformed. The length-based check (NOT a
// `Wrapped == nil` check) is the critical guard — the
// post-decode slice is always non-nil for `wrapped_len = 0`.
func TestApplyRotation_EnableStorageEnvelope_RejectsNonEmptyWrapped(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("non-empty"), // must be len 0
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 1},
	})
	if err == nil {
		t.Fatal("expected halt on non-empty Wrapped, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableStorageEnvelope_RejectsProposerDEKMismatch
// pins the proposer-registration DEK-pinning symmetry: the
// ProposerRegistration row must target the same DEK as the cutover
// payload, otherwise an empty-Wrapped entry would silently insert
// a writer-registry row against an unrelated DEK.
func TestApplyRotation_EnableStorageEnvelope_RejectsProposerDEKMismatch(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 99, FullNodeID: 0xAAAA, LocalEpoch: 1}, // mismatched
	})
	if err == nil {
		t.Fatal("expected halt on proposer DEK mismatch, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableStorageEnvelope_StaleDEKID_BenignNoOp
// pins the §2.1 constraint #3 race posture: a RotateDEK race that
// advances Active.Storage between cutover propose and apply MUST
// NOT halt the FSM. Instead the apply path consumes the entry
// (advance RaftAppliedIndex), leaves StorageEnvelopeActive false,
// and returns nil — the 6D-6 ride-along will surface
// ErrCutoverDEKIDStale to the RPC client without bricking the
// cluster.
func TestApplyRotation_EnableStorageEnvelope_StaleDEKID_BenignNoOp(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	// Stage a RotateDEK that advances Active.Storage from 7 to 99 —
	// simulates the "RotateDEK raced between cutover propose and
	// apply" scenario.
	if err := app.ApplyRotation(200, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                99,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("new-storage-dek"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 99, FullNodeID: 0xAAAA, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("ApplyRotation RotateDEK: %v", err)
	}
	// Now the cutover lands but still carries the old DEKID=7.
	const staleIdx uint64 = 300
	err := app.ApplyRotation(staleIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7, // stale — sidecar.Active.Storage moved to 99
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 2},
	})
	if err != nil {
		t.Fatalf("stale cutover MUST be a benign no-op (no halt), got: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	// Cutover MUST NOT have flipped on a stale DEKID.
	if sc.StorageEnvelopeActive {
		t.Error("StorageEnvelopeActive = true after stale cutover, want false")
	}
	if sc.StorageEnvelopeCutoverIndex != 0 {
		t.Errorf("StorageEnvelopeCutoverIndex = %d after stale cutover, want 0",
			sc.StorageEnvelopeCutoverIndex)
	}
	// But the entry MUST be consumed so it is not replayed.
	if sc.RaftAppliedIndex != staleIdx {
		t.Errorf("RaftAppliedIndex = %d after stale cutover, want %d (entry must be consumed)",
			sc.RaftAppliedIndex, staleIdx)
	}
	// And Active.Storage stays at the post-RotateDEK value.
	if sc.Active.Storage != 99 {
		t.Errorf("Active.Storage = %d, want 99", sc.Active.Storage)
	}
}

// TestApplyRotation_EnableStorageEnvelope_AlreadyActive_IdempotentNoOp
// pins the §2.1 constraint #4 idempotency: a duplicate cutover
// entry consumed after the first cutover already committed MUST
// preserve the original StorageEnvelopeCutoverIndex (the stable
// idempotency token surfaced by the 6D-6 RPC retry path) and not
// re-flip StorageEnvelopeActive. RaftAppliedIndex still advances
// so the duplicate is consumed.
func TestApplyRotation_EnableStorageEnvelope_AlreadyActive_IdempotentNoOp(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	const firstIdx uint64 = 500
	if err := app.ApplyRotation(firstIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("first cutover: %v", err)
	}
	// Duplicate cutover at a higher raft index — simulates the
	// "operator retried after a network blip and the first call
	// already committed" overlap window described in §2.1
	// constraint #4.
	const duplicateIdx uint64 = 600
	err := app.ApplyRotation(duplicateIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 2},
	})
	if err != nil {
		t.Fatalf("duplicate cutover MUST be a benign no-op, got: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if !sc.StorageEnvelopeActive {
		t.Error("StorageEnvelopeActive = false after duplicate, want true")
	}
	// The §6.4 stable idempotency token — original cutover index
	// MUST be preserved across the duplicate apply.
	if sc.StorageEnvelopeCutoverIndex != firstIdx {
		t.Errorf("StorageEnvelopeCutoverIndex = %d after duplicate, want %d (original cutover index must not be overwritten)",
			sc.StorageEnvelopeCutoverIndex, firstIdx)
	}
	// RaftAppliedIndex advances so the duplicate is consumed.
	if sc.RaftAppliedIndex != duplicateIdx {
		t.Errorf("RaftAppliedIndex = %d after duplicate, want %d",
			sc.RaftAppliedIndex, duplicateIdx)
	}
}

// cutoverBootstrapStorageDEKID / cutoverBootstrapRaftDEKID are the
// DEK IDs landed by every cutover-test bootstrap. The cutover
// tests pin Active.Storage to this constant so the fresh-success,
// stale-DEKID, and idempotent-retry cases share the same prelude.
const (
	cutoverBootstrapStorageDEKID uint32 = 7
	cutoverBootstrapRaftDEKID    uint32 = 8
)

// TestApplyRotation_EnableStorageEnvelope_RegistrationBeforeSidecar
// pins the crash-recovery ordering invariant in
// applyEnableStorageEnvelope (gemini-code-assist medium #1 on
// PR804): the ProposerRegistration insert MUST happen BEFORE the
// sidecar flip, so a crash between the two on-disk operations
// leaves the cluster in a state from which FSM replay can recover
// cleanly. The reverse ordering would leave StorageEnvelopeActive
// = true with the registry row missing — the §5.2 startup guard
// would then refuse boot, and a replay would short-circuit on the
// already-active no-op and never insert the row.
//
// The test injects a failing SetRegistryRow on the cutover's
// registry call and asserts:
//
//   - The apply call returns an error (so the FSM halts and the
//     entry is not consumed without consequence).
//   - The sidecar's StorageEnvelopeActive stays false (no
//     premature flip).
//   - The sidecar's StorageEnvelopeCutoverIndex stays 0.
//
// The replay-still-works property (registration insert is
// idempotent via §4.1 case 2-idempotent) is already covered by
// TestApplyRegistration_Case2_Idempotent and is not re-asserted
// here.
func TestApplyRotation_EnableStorageEnvelope_RegistrationBeforeSidecar(t *testing.T) {
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
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: cutoverBootstrapStorageDEKID, WrappedStorage: []byte("s"),
		RaftDEKID: cutoverBootstrapRaftDEKID, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: cutoverBootstrapStorageDEKID, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	// Inject a registry SetRegistryRow failure on the next call.
	// The cutover would otherwise hit §4.1 case 2 (monotonic
	// advance from epoch 0 → 1), so the insert reaches the
	// (failing) SetRegistryRow.
	reg.setEr = errors.New("simulated registry failure mid-apply")
	err = app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:   cutoverBootstrapStorageDEKID,
		Purpose: fsmwire.PurposeStorage,
		Wrapped: []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: cutoverBootstrapStorageDEKID, FullNodeID: 0xAAAA, LocalEpoch: 1,
		},
	})
	if err == nil {
		t.Fatal("expected error from injected registry failure, got nil")
	}
	// Sidecar must NOT show a pre-flipped cutover state — if it
	// did, the §5.2 startup guard would refuse boot on restart,
	// and replay would short-circuit on already-active.
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.StorageEnvelopeActive {
		t.Error("StorageEnvelopeActive flipped despite registration failure (ordering regression)")
	}
	if sc.StorageEnvelopeCutoverIndex != 0 {
		t.Errorf("StorageEnvelopeCutoverIndex = %d despite registration failure, want 0",
			sc.StorageEnvelopeCutoverIndex)
	}
}

// bootstrappedDir constructs a temp dir + sidecar path with an
// Applier-bootstrapped sidecar at
// (cutoverBootstrapStorageDEKID, cutoverBootstrapRaftDEKID).
// Used by the cutover negative tests to share the setup.
func bootstrappedDir(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	app, err := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: cutoverBootstrapStorageDEKID, WrappedStorage: []byte("s"),
		RaftDEKID: cutoverBootstrapRaftDEKID, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: cutoverBootstrapStorageDEKID, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	return dir, sidecarPath
}

// newCutoverApplier returns a fresh Applier wired against the
// bootstrappedDir sidecar so cutover tests do not have to share
// state with the bootstrap call.
func newCutoverApplier(t *testing.T, _ string, sidecarPath string) *encryption.Applier {
	t.Helper()
	app, err := encryption.NewApplier(newMapRegistryStore(),
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(encryption.NewKeystore()),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	return app
}

// assertCutoverNotApplied verifies that a rejected cutover left
// the sidecar's cutover state untouched — neither
// StorageEnvelopeActive nor StorageEnvelopeCutoverIndex may have
// been mutated by a malformed entry whose apply was halted.
func assertCutoverNotApplied(t *testing.T, sidecarPath string) {
	t.Helper()
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.StorageEnvelopeActive {
		t.Error("StorageEnvelopeActive = true despite halt, want false")
	}
	if sc.StorageEnvelopeCutoverIndex != 0 {
		t.Errorf("StorageEnvelopeCutoverIndex = %d despite halt, want 0",
			sc.StorageEnvelopeCutoverIndex)
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
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1"),
		RaftDEKID: 2, WrappedRaft: []byte("r2"),
	}); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	// Second bootstrap with different DEK IDs MUST be rejected.
	err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
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
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s1-orig"),
		RaftDEKID: 2, WrappedRaft: []byte("r2-orig"),
	}); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	// Second bootstrap with same DEK IDs but different wrapped bytes.
	err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
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
	err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
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
	err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
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
	if err := app.ApplyBootstrap(0, fsmwire.BootstrapPayload{
		StorageDEKID: 1, WrappedStorage: []byte("s"),
		RaftDEKID: 2, WrappedRaft: []byte("r"),
	}); err != nil {
		t.Fatalf("pre-bootstrap: %v", err)
	}
	err := app.ApplyRotation(0, fsmwire.RotationPayload{
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

// --- Stage 6D-6c-1: in-memory accessor coverage ---
//
// The Applier exposes ActiveStorageKeyID() and StorageEnvelopeActive()
// so main.go can wire them as the per-Put closures into
// store.WithEncryption / store.WithStorageEnvelopeGate without
// ReadSidecar-on-every-Put. Hot-path correctness requires:
//
//   - Pre-bootstrap: (0, false) and false. Storage layer must
//     write cleartext.
//   - Post-bootstrap: (StorageDEKID, true) and false. Encryption
//     wraps payloads at-rest using the storage DEK, but the
//     §4.1 envelope is still gated off.
//   - Post-rotate-dek-storage: returns the NEW storage DEK id,
//     and StorageEnvelopeActive unchanged.
//   - Post-cutover: ActiveStorageKeyID unchanged, but the gate
//     flips to true.
//   - NewApplier primes both atomics from the on-disk sidecar so
//     a freshly-started process serves correct accessor values
//     before the FSM has replayed a single entry.
//   - Concurrent readers under -race must observe coherent values
//     (atomic.Bool / atomic.Uint32 guarantees).

func TestApplier_StorageAccessors_PreBootstrap(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, err := encryption.NewApplier(newMapRegistryStore(),
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(encryption.NewKeystore()),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	id, ok := app.ActiveStorageKeyID()
	if ok || id != 0 {
		t.Errorf("ActiveStorageKeyID pre-bootstrap = (%d, %v), want (0, false)", id, ok)
	}
	if app.StorageEnvelopeActive() {
		t.Error("StorageEnvelopeActive pre-bootstrap = true, want false")
	}
}

func TestApplier_StorageAccessors_PostBootstrap(t *testing.T) {
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
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 11, WrappedStorage: []byte("s"),
		RaftDEKID: 22, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 11, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	id, ok := app.ActiveStorageKeyID()
	if !ok || id != 11 {
		t.Errorf("ActiveStorageKeyID post-bootstrap = (%d, %v), want (11, true)", id, ok)
	}
	// Cutover gate must NOT flip just because bootstrap landed —
	// §7.1 Phase 1 requires an explicit EnableStorageEnvelope
	// proposal.
	if app.StorageEnvelopeActive() {
		t.Error("StorageEnvelopeActive post-bootstrap = true, want false")
	}
}

func TestApplier_StorageAccessors_PostRotateDEK(t *testing.T) {
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
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 11, WrappedStorage: []byte("s"),
		RaftDEKID: 22, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 11, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	// Rotate storage DEK 11 -> 33.
	if err := app.ApplyRotation(200, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                33,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte("w33"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 33, FullNodeID: 0xAAAA, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("ApplyRotation rotate-dek: %v", err)
	}
	id, ok := app.ActiveStorageKeyID()
	if !ok || id != 33 {
		t.Errorf("ActiveStorageKeyID post-rotate = (%d, %v), want (33, true)", id, ok)
	}
	if app.StorageEnvelopeActive() {
		t.Error("StorageEnvelopeActive flipped by rotate-dek, want false (rotate is a key swap, not a cutover)")
	}
}

func TestApplier_StorageAccessors_PostCutover(t *testing.T) {
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
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 7, WrappedStorage: []byte("s"),
		RaftDEKID: 8, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	if err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                7,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 7, FullNodeID: 0xAAAA, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("ApplyRotation cutover: %v", err)
	}
	id, ok := app.ActiveStorageKeyID()
	if !ok || id != 7 {
		t.Errorf("ActiveStorageKeyID post-cutover = (%d, %v), want (7, true) — cutover must not re-point Active", id, ok)
	}
	if !app.StorageEnvelopeActive() {
		t.Error("StorageEnvelopeActive post-cutover = false, want true")
	}
}

// TestApplier_StorageAccessors_PrimedFromExistingSidecar pins the
// startup-recovery contract: a process restart constructs a fresh
// Applier against an already-populated sidecar (the previous
// process's apply landed). The accessors must reflect the on-disk
// state BEFORE the FSM has replayed a single entry, because the
// storage layer's Put path queries them immediately after Pebble
// open.
func TestApplier_StorageAccessors_PrimedFromExistingSidecar(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	// First Applier writes a full post-cutover sidecar.
	reg := newMapRegistryStore()
	first, err := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(encryption.NewKeystore()),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier (first): %v", err)
	}
	if err := first.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 9, WrappedStorage: []byte("s"),
		RaftDEKID: 10, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 9, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("first ApplyBootstrap: %v", err)
	}
	if err := first.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                9,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 9, FullNodeID: 0xAAAA, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("first ApplyRotation cutover: %v", err)
	}

	// Second Applier — simulates a process restart. The on-disk
	// sidecar holds Active.Storage=9 and StorageEnvelopeActive=true.
	// We use a *fresh* mapRegistryStore so this is unambiguously a
	// from-disk prime, not state inherited from `first`.
	second, err := encryption.NewApplier(newMapRegistryStore(),
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(encryption.NewKeystore()),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier (second): %v", err)
	}
	id, ok := second.ActiveStorageKeyID()
	if !ok || id != 9 {
		t.Errorf("second ActiveStorageKeyID = (%d, %v), want (9, true)", id, ok)
	}
	if !second.StorageEnvelopeActive() {
		t.Error("second StorageEnvelopeActive = false, want true (must prime from disk)")
	}
}

// TestApplier_StorageAccessors_SharedCache_AcrossAppliers pins the
// multi-shard correctness invariant: in a multi-group deployment,
// main.go constructs one Applier per shard but encryption FSM
// entries apply on exactly one shard (the one whose engine accepted
// the proposal). The remaining per-shard Appliers must still
// observe the post-apply state, because their per-Put storage
// closures gate writes on the SAME cluster-wide encryption state.
//
// Without a shared StateCache (the original 6D-6c-1 design), each
// Applier carried its own atomics and only the apply-receiving
// shard would surface (id, true) — every other shard's storage
// layer would see (0, false) and silently write cleartext after
// bootstrap, or skip the §4.1 envelope wrap after cutover. This
// test fails on the original per-Applier-atomics design and passes
// once the shared StateCache lands.
//
// Reported as a P1 by chatgpt-codex-connector on PR #821
// (review comment r3294696832).
func TestApplier_StorageAccessors_SharedCache_AcrossAppliers(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	shared := encryption.NewStateCache()
	appliers := buildSharedCacheAppliers(t, sidecarPath, shared, 2)

	// Apply Bootstrap on appliers[0] only — models the FSM apply
	// landing on shard 0's leader. Shard 1's applier never runs the
	// apply path, but its accessors MUST surface the update because
	// they read from the same StateCache.
	if err := appliers[0].ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 17, WrappedStorage: []byte("s"),
		RaftDEKID: 18, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 17, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("appliers[0].ApplyBootstrap: %v", err)
	}
	assertAllAppliersAgree(t, appliers, 17, false)

	// Now flip the cutover via appliers[1] (models the cutover entry
	// landing on the OTHER shard than the bootstrap). appliers[0]
	// must still see the gate flip via the shared cache.
	if err := appliers[1].ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableStorageEnvelope,
		DEKID:                17,
		Purpose:              fsmwire.PurposeStorage,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: 17, FullNodeID: 0xAAAA, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("appliers[1].ApplyRotation cutover: %v", err)
	}
	assertAllAppliersAgree(t, appliers, 17, true)

	// Reading via the StateCache directly (the API main.go will use
	// for the per-Put storage closures in 6D-6c-2) must reflect the
	// same state regardless of which Applier ran the apply.
	if id, ok := shared.ActiveStorageKeyID(); !ok || id != 17 {
		t.Errorf("shared.ActiveStorageKeyID = (%d, %v), want (17, true)", id, ok)
	}
	if !shared.StorageEnvelopeActive() {
		t.Error("shared.StorageEnvelopeActive = false, want true after cutover")
	}
}

// buildSharedCacheAppliers wires N appliers to the same sidecar
// path and the same shared StateCache, modelling main.go's
// buildShardGroups per-shard Applier construction.
func buildSharedCacheAppliers(
	t *testing.T,
	sidecarPath string,
	shared *encryption.StateCache,
	n int,
) []*encryption.Applier {
	t.Helper()
	appliers := make([]*encryption.Applier, n)
	for i := range appliers {
		a, err := encryption.NewApplier(newMapRegistryStore(),
			encryption.WithKEK(&fakeKEK{}),
			encryption.WithKeystore(encryption.NewKeystore()),
			encryption.WithSidecarPath(sidecarPath),
			encryption.WithStateCache(shared),
		)
		if err != nil {
			t.Fatalf("NewApplier[%d]: %v", i, err)
		}
		appliers[i] = a
	}
	return appliers
}

// assertAllAppliersAgree verifies every applier surfaces the same
// (ActiveStorageKeyID, StorageEnvelopeActive) state — the shared-
// cache invariant for the cross-shard apply propagation.
func assertAllAppliersAgree(
	t *testing.T,
	appliers []*encryption.Applier,
	wantID uint32,
	wantEnvelopeActive bool,
) {
	t.Helper()
	for i, a := range appliers {
		id, ok := a.ActiveStorageKeyID()
		if !ok || id != wantID {
			t.Errorf("appliers[%d].ActiveStorageKeyID = (%d, %v), want (%d, true)", i, id, ok, wantID)
		}
		if got := a.StorageEnvelopeActive(); got != wantEnvelopeActive {
			t.Errorf("appliers[%d].StorageEnvelopeActive = %v, want %v", i, got, wantEnvelopeActive)
		}
	}
}

// TestStateCache_NilSafe pins the nil-receiver contract so callers
// that have not yet wired a StateCache (early startup, partial
// option configuration) get the pre-bootstrap posture rather than
// a nil-deref panic.
func TestStateCache_NilSafe(t *testing.T) {
	t.Parallel()
	var c *encryption.StateCache
	if id, ok := c.ActiveStorageKeyID(); ok || id != 0 {
		t.Errorf("nil StateCache.ActiveStorageKeyID = (%d, %v), want (0, false)", id, ok)
	}
	if c.StorageEnvelopeActive() {
		t.Error("nil StateCache.StorageEnvelopeActive = true, want false")
	}
	if c.Registered() {
		t.Error("nil StateCache.Registered = true, want false")
	}
	c.MarkRegistered(7)                         // must not panic
	c.RefreshFromSidecar(&encryption.Sidecar{}) // must not panic
}

// TestStateCache_Registered exercises the Stage 7a-2 §4.1 direct-path
// gate predicate: Registered() is true only when an active storage DEK
// is present AND MarkRegistered has marked that exact id. A 7b-style
// rotate-dek to a new active id automatically re-arms the gate.
func TestStateCache_Registered(t *testing.T) {
	t.Parallel()
	c := encryption.NewStateCache()

	// No active DEK → never registered, even after a stray MarkRegistered.
	if c.Registered() {
		t.Error("fresh cache Registered() = true, want false")
	}
	c.MarkRegistered(0) // no-op: zero id is the "no DEK" sentinel
	if c.Registered() {
		t.Error("after MarkRegistered(0) Registered() = true, want false")
	}

	// Activate DEK 7 but don't mark it → still false.
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion}
	sc.Active.Storage = 7
	c.RefreshFromSidecar(sc)
	if c.Registered() {
		t.Error("active DEK 7 unmarked Registered() = true, want false")
	}

	// Mark 7 → registered.
	c.MarkRegistered(7)
	if !c.Registered() {
		t.Error("active DEK 7 marked Registered() = false, want true")
	}

	// Rotate active DEK to 8 (7b) → gate re-arms until 8 is marked.
	sc.Active.Storage = 8
	c.RefreshFromSidecar(sc)
	if c.Registered() {
		t.Error("after rotate to DEK 8 (8 unmarked) Registered() = true, want false")
	}
	c.MarkRegistered(8)
	if !c.Registered() {
		t.Error("active DEK 8 marked Registered() = false, want true")
	}
}

// TestApplier_StorageAccessors_ConcurrentReads exercises the
// atomic.Uint32 / atomic.Bool seam under -race. The accessors are
// called on the hot storage Put path, so a torn read or a missed
// happens-before edge with the apply-side store would be a
// production correctness bug. We run many concurrent reader
// goroutines alongside a single applier goroutine and verify
// readers only ever observe one of the two valid states (zero or
// the post-cutover state) — never a torn intermediate.
func TestApplier_StorageAccessors_ConcurrentReads(t *testing.T) {
	t.Parallel()
	reg := newMapRegistryStore()
	dir := t.TempDir()
	sidecarPath := dir + "/keys.json"
	app, err := encryption.NewApplier(reg,
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(encryption.NewKeystore()),
		encryption.WithSidecarPath(sidecarPath),
	)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}

	const wantDEKID uint32 = 41
	const readers = 8
	const readsPerGoroutine = 2000

	start := make(chan struct{})
	done := make(chan struct{}, readers)
	for i := 0; i < readers; i++ {
		go concurrentAccessorReader(t, app, wantDEKID, readsPerGoroutine, start, done)
	}
	close(start)
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: wantDEKID, WrappedStorage: []byte("s"),
		RaftDEKID: 42, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: wantDEKID, FullNodeID: 0xAAAA, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	for i := 0; i < readers; i++ {
		<-done
	}
	id, ok := app.ActiveStorageKeyID()
	if !ok || id != wantDEKID {
		t.Errorf("final ActiveStorageKeyID = (%d, %v), want (%d, true)", id, ok, wantDEKID)
	}
}

// concurrentAccessorReader spins on ActiveStorageKeyID until it
// observes either the pre-bootstrap zero state or the
// post-bootstrap (wantDEKID, true) state. Any other combination
// indicates a torn read across the (uint32 id, bool ok) pair and
// fails the test. Extracted out of
// TestApplier_StorageAccessors_ConcurrentReads to keep that test
// under cyclop's complexity ceiling.
func concurrentAccessorReader(
	t *testing.T,
	app *encryption.Applier,
	wantDEKID uint32,
	reads int,
	start <-chan struct{},
	done chan<- struct{},
) {
	t.Helper()
	defer func() { done <- struct{}{} }()
	<-start
	for j := 0; j < reads; j++ {
		id, ok := app.ActiveStorageKeyID()
		if !validAccessorObservation(id, ok, wantDEKID) {
			t.Errorf("torn ActiveStorageKeyID read: (%d, %v)", id, ok)
			return
		}
		_ = app.StorageEnvelopeActive()
	}
}

// validAccessorObservation accepts only the two coherent states
// the readers may witness while the bootstrap apply is in flight:
// (0, false) pre-flip and (wantDEKID, true) post-flip.
func validAccessorObservation(id uint32, ok bool, wantDEKID uint32) bool {
	if !ok && id == 0 {
		return true
	}
	if ok && id == wantDEKID {
		return true
	}
	return false
}

// --- Stage 6E-1: RotateSubEnableRaftEnvelope tests ---
// Mirror the storage-envelope cutover tests but target the raft slot
// + the RaftEnvelopeCutoverIndex (uint64 index, not bool flag).

// TestApplyRotation_EnableRaftEnvelope_FreshSuccess pins Stage 6E-1
// happy-path: after Bootstrap, a cutover entry with
// DEKID == Active.Raft, Purpose == PurposeRaft, empty Wrapped sets
// RaftEnvelopeCutoverIndex = raftIdx inside one sidecar fsync. The
// ProposerRegistration row is inserted via the standard §4.1 case
// dispatch covering the proposer's first encrypted write under the
// now-active raft envelope.
func TestApplyRotation_EnableRaftEnvelope_FreshSuccess(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		wrapped []byte
	}{
		{name: "nil_wrapped", wrapped: nil},
		{name: "empty_wrapped", wrapped: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runRaftCutoverFreshSuccess(t, tc.wrapped)
		})
	}
}

func runRaftCutoverFreshSuccess(t *testing.T, wrapped []byte) {
	t.Helper()
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
	if err := app.ApplyBootstrap(100, fsmwire.BootstrapPayload{
		StorageDEKID: 7, WrappedStorage: []byte("s"),
		RaftDEKID: 8, WrappedRaft: []byte("r"),
		BatchRegistry: []fsmwire.RegistrationPayload{
			{DEKID: 8, FullNodeID: 0xBBBB, LocalEpoch: 0},
		},
	}); err != nil {
		t.Fatalf("ApplyBootstrap: %v", err)
	}
	const raftCutoverIdx uint64 = 600
	if err := app.ApplyRotation(raftCutoverIdx, fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:   8, // matches sidecar.Active.Raft
		Purpose: fsmwire.PurposeRaft,
		Wrapped: wrapped,
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: 8, FullNodeID: 0xBBBB, LocalEpoch: 1,
		},
	}); err != nil {
		t.Fatalf("ApplyRotation raft-cutover: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftEnvelopeCutoverIndex != raftCutoverIdx {
		t.Errorf("RaftEnvelopeCutoverIndex = %d, want %d",
			sc.RaftEnvelopeCutoverIndex, raftCutoverIdx)
	}
	if sc.RaftAppliedIndex != raftCutoverIdx {
		t.Errorf("RaftAppliedIndex = %d, want %d",
			sc.RaftAppliedIndex, raftCutoverIdx)
	}
	if sc.Active.Raft != 8 {
		t.Errorf("Active.Raft = %d, want 8 (raft-cutover should not re-point)",
			sc.Active.Raft)
	}
	// Storage cutover state must remain untouched — raft-envelope
	// cutover is independent of storage-envelope cutover.
	if sc.StorageEnvelopeActive {
		t.Error("StorageEnvelopeActive flipped by raft-cutover, want unchanged")
	}
	if sc.StorageEnvelopeCutoverIndex != 0 {
		t.Errorf("StorageEnvelopeCutoverIndex = %d, want 0 (raft-cutover should not touch storage)",
			sc.StorageEnvelopeCutoverIndex)
	}
	assertProposerEpoch(t, reg, 8, 0xBBBB, 1)
}

// TestApplyRotation_EnableRaftEnvelope_RejectsNonRaftPurpose pins
// payload constraint #1: Purpose == PurposeStorage on a
// raft-envelope cutover entry MUST halt apply rather than silently
// mis-routing the cutover through the wrong slot accounting.
func TestApplyRotation_EnableRaftEnvelope_RejectsNonRaftPurpose(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeStorage, // wrong
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	})
	if err == nil {
		t.Fatal("expected halt on non-raft purpose, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertRaftCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableRaftEnvelope_RejectsNonEmptyWrapped pins
// payload constraint #2: a raft-envelope cutover carries
// `Wrapped` empty (the active DEK is referenced by DEKID, not
// re-shipped). A non-empty Wrapped is malformed and must halt
// apply.
func TestApplyRotation_EnableRaftEnvelope_RejectsNonEmptyWrapped(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{0xDE, 0xAD, 0xBE, 0xEF}, // non-empty
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	})
	if err == nil {
		t.Fatal("expected halt on non-empty Wrapped, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertRaftCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableRaftEnvelope_RejectsProposerDEKMismatch
// pins payload constraint #3: ProposerRegistration.DEKID MUST
// equal payload DEKID. A mismatch would silently insert a
// writer-registry row against an unrelated DEK while leaving the
// cutover-target DEK unregistered — the same shape that the
// rotate-dek variant guards against.
func TestApplyRotation_EnableRaftEnvelope_RejectsProposerDEKMismatch(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(500, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID + 1, FullNodeID: 0xBBBB, LocalEpoch: 1},
	})
	if err == nil {
		t.Fatal("expected halt on proposer dek mismatch, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertRaftCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableRaftEnvelope_RejectsZeroRaftIndex pins a
// fail-closed pre-check: `RaftEnvelopeCutoverIndex != 0` is the
// sole "Phase-2 active" sentinel for the raft variant (unlike the
// storage variant which carries a separate `StorageEnvelopeActive`
// bool). If the apply path ever receives raftIdx == 0 — a sentinel
// elsewhere meaning "no index supplied" — the fresh-success branch
// would persist the proposer-registration row, leave
// `RaftEnvelopeCutoverIndex` at 0 (logically inactive), and return
// nil, while replays would re-enter the fresh-success branch since
// the already-active short-circuit only fires on `!= 0`. Reject
// raftIdx == 0 before any sidecar mutation so the apply halts.
func TestApplyRotation_EnableRaftEnvelope_RejectsZeroRaftIndex(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	err := app.ApplyRotation(0, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	})
	if err == nil {
		t.Fatal("expected halt on raftIdx == 0, got nil")
	}
	if !errors.Is(err, encryption.ErrEncryptionApply) {
		t.Errorf("err not marked ErrEncryptionApply: %v", err)
	}
	assertRaftCutoverNotApplied(t, sidecarPath)
}

// TestApplyRotation_EnableRaftEnvelope_StaleDEKID_BenignNoOp pins
// constraint #3 from the design: a RotateDEK race between propose
// and apply that advances sidecar.Active.Raft past payload DEKID
// is a benign no-op. Advance RaftAppliedIndex (so the entry is
// not replayed) but do NOT mutate RaftEnvelopeCutoverIndex.
func TestApplyRotation_EnableRaftEnvelope_StaleDEKID_BenignNoOp(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	// Stage a RotateDEK that advances Active.Raft from
	// cutoverBootstrapRaftDEKID to a new id — simulates "RotateDEK
	// raced between cutover propose and apply" (mirrors the
	// storage stale-DEKID test setup). The RotateDEK path
	// crash-durably writes both Active.Raft AND the keys[] entry
	// so the sidecar validator accepts the new state.
	const newRaftDEKID uint32 = 99
	if err := app.ApplyRotation(200, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                newRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte("new-raft-dek"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: newRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("ApplyRotation RotateDEK: %v", err)
	}
	// Now the cutover lands but still carries the stale DEKID.
	const staleIdx uint64 = 300
	if err := app.ApplyRotation(staleIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID, // stale — Active.Raft moved to newRaftDEKID
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 2},
	}); err != nil {
		t.Fatalf("stale-DEKID raft-cutover MUST be a benign no-op (no halt), got: %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftEnvelopeCutoverIndex != 0 {
		t.Errorf("RaftEnvelopeCutoverIndex = %d on stale-DEKID no-op, want 0",
			sc.RaftEnvelopeCutoverIndex)
	}
	if sc.RaftAppliedIndex != staleIdx {
		t.Errorf("RaftAppliedIndex = %d, want %d (no-op must still advance applied)",
			sc.RaftAppliedIndex, staleIdx)
	}
	if sc.Active.Raft != newRaftDEKID {
		t.Errorf("Active.Raft = %d, want %d", sc.Active.Raft, newRaftDEKID)
	}
}

// TestApplyRotation_EnableRaftEnvelope_AlreadyActive_IdempotentNoOp
// pins constraint #4: a duplicate cutover entry (Phase-2 already
// active) is idempotent. The original RaftEnvelopeCutoverIndex
// (set by the first apply) is preserved as the stable
// idempotency token; only RaftAppliedIndex advances so the
// duplicate is not replayed. The ProposerRegistration field is
// intentionally NOT re-inserted — the first apply ran
// ApplyRegistration before WriteSidecar, so the invariant
// "RaftEnvelopeCutoverIndex != 0 ⇒ registration row already on
// disk" holds across crash-restart.
func TestApplyRotation_EnableRaftEnvelope_AlreadyActive_IdempotentNoOp(t *testing.T) {
	t.Parallel()
	dir, sidecarPath := bootstrappedDir(t)
	app := newCutoverApplier(t, dir, sidecarPath)
	const firstIdx uint64 = 500
	if err := app.ApplyRotation(firstIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("first cutover: %v", err)
	}
	const duplicateIdx uint64 = 501
	if err := app.ApplyRotation(duplicateIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 2},
	}); err != nil {
		t.Fatalf("duplicate cutover must be idempotent no-op, got %v", err)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftEnvelopeCutoverIndex != firstIdx {
		t.Errorf("RaftEnvelopeCutoverIndex = %d on duplicate, want %d (first-apply value must be preserved)",
			sc.RaftEnvelopeCutoverIndex, firstIdx)
	}
	if sc.RaftAppliedIndex != duplicateIdx {
		t.Errorf("RaftAppliedIndex = %d, want %d (duplicate must still advance applied)",
			sc.RaftAppliedIndex, duplicateIdx)
	}
}

// recordingRaftCutoverWrapInstaller captures every invocation so the
// 6E-2e-1 tests can pin (a) how many times the installer fired, and
// (b) the arguments (cutover_idx, active_raft_dek_id) it received
// on each call. errToReturn lets a test exercise the halt-apply
// error propagation contract.
type recordingRaftCutoverWrapInstaller struct {
	calls       []recordingRaftCutoverWrapInstallerCall
	errToReturn error
}

type recordingRaftCutoverWrapInstallerCall struct {
	cutoverIdx      uint64
	activeRaftDEKID uint32
}

func (r *recordingRaftCutoverWrapInstaller) install(cutoverIdx uint64, activeRaftDEKID uint32) error {
	r.calls = append(r.calls, recordingRaftCutoverWrapInstallerCall{
		cutoverIdx:      cutoverIdx,
		activeRaftDEKID: activeRaftDEKID,
	})
	return r.errToReturn
}

// newCutoverApplierWithInstaller mirrors newCutoverApplier but also
// wires a recording installer through the Stage 6E-2e-1
// WithRaftCutoverWrapInstaller option. Returns both the Applier and
// the recorder so tests can assert on call shape.
func newCutoverApplierWithInstaller(t *testing.T, sidecarPath string) (*encryption.Applier, *recordingRaftCutoverWrapInstaller) {
	t.Helper()
	rec := &recordingRaftCutoverWrapInstaller{}
	app := newCutoverApplierWithOpts(t, sidecarPath,
		encryption.WithRaftCutoverWrapInstaller(rec.install))
	return app, rec
}

// newCutoverApplierWithOpts returns an Applier wired with the
// shared cutover fixtures (KEK, keystore, sidecar) plus any
// extra options the test supplies. Reuses the same bootstrap
// fixtures the parent newCutoverApplier helper uses.
func newCutoverApplierWithOpts(t *testing.T, sidecarPath string, extra ...encryption.ApplierOption) *encryption.Applier {
	t.Helper()
	reg := newMapRegistryStore()
	ks := encryption.NewKeystore()
	opts := []encryption.ApplierOption{
		encryption.WithKEK(&fakeKEK{}),
		encryption.WithKeystore(ks),
		encryption.WithSidecarPath(sidecarPath),
	}
	opts = append(opts, extra...)
	app, err := encryption.NewApplier(reg, opts...)
	if err != nil {
		t.Fatalf("NewApplier: %v", err)
	}
	return app
}

// TestApplyRotation_EnableRaftEnvelope_FreshSuccess_InvokesInstaller
// pins the BLOCKER (b) contract from codex P1 round-3 on PR933: on
// a fresh-success apply of the cutover marker, the Applier MUST
// invoke the registered wrap installer exactly once with the
// just-stamped cutover index and the active raft DEK ID. The
// installer is what publishes the wrap closure to every
// participating kv.ShardGroup on this replica, so a follower that
// becomes leader post-cutover has wrap active without needing the
// EnableRaftEnvelope handler to re-run.
func TestApplyRotation_EnableRaftEnvelope_FreshSuccess_InvokesInstaller(t *testing.T) {
	t.Parallel()
	_, sidecarPath := bootstrappedDir(t)
	app, rec := newCutoverApplierWithInstaller(t, sidecarPath)
	const raftCutoverIdx uint64 = 600
	if err := app.ApplyRotation(raftCutoverIdx, fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:   cutoverBootstrapRaftDEKID,
		Purpose: fsmwire.PurposeRaft,
		Wrapped: []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1,
		},
	}); err != nil {
		t.Fatalf("ApplyRotation raft-cutover: %v", err)
	}
	if got, want := len(rec.calls), 1; got != want {
		t.Fatalf("installer called %d times on fresh-success, want %d", got, want)
	}
	if got := rec.calls[0].cutoverIdx; got != raftCutoverIdx {
		t.Errorf("installer cutoverIdx = %d, want %d (must equal the just-stamped sidecar value)",
			got, raftCutoverIdx)
	}
	if got, want := rec.calls[0].activeRaftDEKID, cutoverBootstrapRaftDEKID; got != want {
		t.Errorf("installer activeRaftDEKID = %d, want %d", got, want)
	}
}

// TestApplyRotation_EnableRaftEnvelope_AlreadyActive_InvokesInstaller
// pins the FSM-replay safety contract: a duplicate cutover apply
// (sidecar.RaftEnvelopeCutoverIndex already non-zero) MUST still
// invoke the installer so a process restart that replays the
// cutover entry through this branch republishes the wrap closure
// in-process. The original cutover index is passed, not the new
// raftIdx — the wrap closure semantics must match the value the
// §6.3 strict-`>` apply hook compares against on every replica.
func TestApplyRotation_EnableRaftEnvelope_AlreadyActive_InvokesInstaller(t *testing.T) {
	t.Parallel()
	_, sidecarPath := bootstrappedDir(t)
	app, rec := newCutoverApplierWithInstaller(t, sidecarPath)
	const firstIdx uint64 = 500
	if err := app.ApplyRotation(firstIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("first cutover: %v", err)
	}
	const duplicateIdx uint64 = 501
	if err := app.ApplyRotation(duplicateIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 2},
	}); err != nil {
		t.Fatalf("duplicate cutover: %v", err)
	}
	// One fresh-success + one already-active call = two invocations total.
	if got, want := len(rec.calls), 2; got != want {
		t.Fatalf("installer called %d times across fresh+duplicate, want %d (already-active branch must also invoke for replay safety)", got, want)
	}
	if got := rec.calls[1].cutoverIdx; got != firstIdx {
		t.Errorf("already-active installer cutoverIdx = %d, want %d (the ORIGINAL cutover index, not the duplicate raftIdx)",
			got, firstIdx)
	}
}

// TestApplyRotation_EnableRaftEnvelope_AlreadyActive_StaleDEKReplay_InvokesInstaller
// pins the codex P1 invariant on PR944: an FSM replay of the
// original cutover marker AFTER a successful cutover AND a
// subsequent RotateDEK MUST hit the already-active branch (which
// invokes the installer), NOT the stale-DEK no-op branch. This is
// the exact replay-safety path the installer hook is meant to
// repair.
//
// Scenario:
//  1. Original cutover applied at index N with DEK D1 (Active.Raft=D1).
//     Installer fires, RaftEnvelopeCutoverIndex=N.
//  2. RotateDEK advances Active.Raft from D1 to D2.
//  3. Process restarts. Snapshot didn't include the cutover entry,
//     so FSM replay re-applies it. Replay carries p.DEKID=D1, but
//     sidecar.Active.Raft is now D2.
//  4. Order MUST be: check RaftEnvelopeCutoverIndex != 0 FIRST → hit
//     the already-active branch → installer fires with sc.Active.Raft
//     (D2). Reversed order (stale-DEK first) short-circuits at
//     p.DEKID != D2 and the wrap is never re-installed in this
//     process — a freshly-elected leader on this node would admit
//     cleartext proposals above the cutover index and brick the §6.3
//     strict-`>` apply hook on every replica.
func TestApplyRotation_EnableRaftEnvelope_AlreadyActive_StaleDEKReplay_InvokesInstaller(t *testing.T) {
	t.Parallel()
	_, sidecarPath := bootstrappedDir(t)
	app, rec := newCutoverApplierWithInstaller(t, sidecarPath)

	const originalCutoverIdx uint64 = 500
	if err := app.ApplyRotation(originalCutoverIdx, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("original cutover: %v", err)
	}
	const newRaftDEKID uint32 = 99
	if err := app.ApplyRotation(600, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                newRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte("new-raft-dek"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: newRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 2},
	}); err != nil {
		t.Fatalf("RotateDEK: %v", err)
	}
	callsBeforeReplay := len(rec.calls)
	// FSM replay carries the ORIGINAL payload's DEKID, now stale
	// against the post-rotate Active.Raft.
	if err := app.ApplyRotation(700, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 3},
	}); err != nil {
		t.Fatalf("FSM replay of cutover marker: %v", err)
	}
	if got := len(rec.calls); got != callsBeforeReplay+1 {
		t.Errorf("installer called %d times on replay, want 1 (already-active branch MUST install regardless of replayed payload's DEKID)",
			got-callsBeforeReplay)
	}
	if got := rec.calls[len(rec.calls)-1].activeRaftDEKID; got != newRaftDEKID {
		t.Errorf("installer activeRaftDEKID on replay = %d, want %d (sidecar.Active.Raft, NOT replayed p.DEKID)",
			got, newRaftDEKID)
	}
	if got := rec.calls[len(rec.calls)-1].cutoverIdx; got != originalCutoverIdx {
		t.Errorf("installer cutoverIdx on replay = %d, want %d (originally-recorded sidecar value)",
			got, originalCutoverIdx)
	}
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftEnvelopeCutoverIndex != originalCutoverIdx {
		t.Errorf("RaftEnvelopeCutoverIndex = %d after replay, want %d",
			sc.RaftEnvelopeCutoverIndex, originalCutoverIdx)
	}
	if sc.Active.Raft != newRaftDEKID {
		t.Errorf("Active.Raft = %d after replay, want %d", sc.Active.Raft, newRaftDEKID)
	}
}

// TestApplyRotation_EnableRaftEnvelope_StaleDEKID_SkipsInstaller
// pins the constraint #3 contract: a stale-DEK race produces a
// benign no-op apply that advances RaftAppliedIndex but does NOT
// activate the cutover. The installer MUST NOT fire — the wrap
// closure has no DEK to bind to at this point (Active.Raft has
// moved on), and installing would publish a closure keyed to the
// wrong DEK that the §6.3 hook would then fail to unwrap.
func TestApplyRotation_EnableRaftEnvelope_StaleDEKID_SkipsInstaller(t *testing.T) {
	t.Parallel()
	_, sidecarPath := bootstrappedDir(t)
	app, rec := newCutoverApplierWithInstaller(t, sidecarPath)
	const newRaftDEKID uint32 = 99
	if err := app.ApplyRotation(200, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubRotateDEK,
		DEKID:                newRaftDEKID,
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte("new-raft-dek"),
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: newRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1},
	}); err != nil {
		t.Fatalf("ApplyRotation RotateDEK: %v", err)
	}
	if err := app.ApplyRotation(300, fsmwire.RotationPayload{
		SubTag:               fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:                cutoverBootstrapRaftDEKID, // stale
		Purpose:              fsmwire.PurposeRaft,
		Wrapped:              []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 2},
	}); err != nil {
		t.Fatalf("stale-DEK cutover: %v", err)
	}
	if got := len(rec.calls); got != 0 {
		t.Errorf("installer called %d times on stale-DEK no-op, want 0 (no cutover took effect; wrap installation would key to the wrong DEK)", got)
	}
}

// TestApplyRotation_EnableRaftEnvelope_InstallerError_HaltsApply pins
// the fail-closed contract: if the installer returns an error, the
// applier surfaces it so the FSM apply halts. Sidecar already
// records the cutover (write ordered BEFORE installer call), so
// crash recovery is consistent: process restart sees the cutover
// active, startup-time install (6E-2e-3 main.go wiring) republishes
// the wrap closure, and the next apply hits the already-active
// idempotent branch. The reverse ordering (install first) would
// leave wrap published but sidecar pre-cutover on crash, breaking
// the equality the §6.3 hook relies on cluster-wide.
func TestApplyRotation_EnableRaftEnvelope_InstallerError_HaltsApply(t *testing.T) {
	t.Parallel()
	_, sidecarPath := bootstrappedDir(t)
	rec := &recordingRaftCutoverWrapInstaller{errToReturn: errors.New("synthetic wrap-install failure")}
	app := newCutoverApplierWithOpts(t, sidecarPath,
		encryption.WithRaftCutoverWrapInstaller(rec.install))
	const raftCutoverIdx uint64 = 600
	err := app.ApplyRotation(raftCutoverIdx, fsmwire.RotationPayload{
		SubTag:  fsmwire.RotateSubEnableRaftEnvelope,
		DEKID:   cutoverBootstrapRaftDEKID,
		Purpose: fsmwire.PurposeRaft,
		Wrapped: []byte{},
		ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: cutoverBootstrapRaftDEKID, FullNodeID: 0xBBBB, LocalEpoch: 1,
		},
	})
	if err == nil {
		t.Fatal("ApplyRotation: want installer-error halt, got nil")
	}
	if !strings.Contains(err.Error(), "install raft-cutover wrap") {
		t.Errorf("error %q does not mention the install path", err)
	}
	// Sidecar MUST already reflect the cutover (write ordered
	// before installer call) so recovery via restart converges.
	sc, readErr := encryption.ReadSidecar(sidecarPath)
	if readErr != nil {
		t.Fatalf("ReadSidecar: %v", readErr)
	}
	if sc.RaftEnvelopeCutoverIndex != raftCutoverIdx {
		t.Errorf("RaftEnvelopeCutoverIndex = %d after installer error, want %d (sidecar write MUST precede installer call for crash-recovery convergence)",
			sc.RaftEnvelopeCutoverIndex, raftCutoverIdx)
	}
}

// assertRaftCutoverNotApplied verifies a rejected raft-envelope
// cutover left the cutover sidecar state untouched.
func assertRaftCutoverNotApplied(t *testing.T, sidecarPath string) {
	t.Helper()
	sc, err := encryption.ReadSidecar(sidecarPath)
	if err != nil {
		t.Fatalf("ReadSidecar: %v", err)
	}
	if sc.RaftEnvelopeCutoverIndex != 0 {
		t.Errorf("RaftEnvelopeCutoverIndex = %d despite halt, want 0",
			sc.RaftEnvelopeCutoverIndex)
	}
}
