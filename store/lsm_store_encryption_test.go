package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// encryptedStoreFixture wires a pebbleStore with a one-DEK keystore,
// a deterministic-counter nonce factory, and an "always active" key
// resolver. Stage 5/6 will replace the resolver with a sidecar-backed
// one; Stage 2 just needs a knob the test can flip.
//
// Tests that simulate a disk attacker (AAD binding, tag tamper, etc.)
// need to close the store, mutate Pebble bytes directly, then reopen.
// The fixture's Cleanup-registered close therefore goes through
// `closeIfOpen`, which is idempotent so the test can call it
// explicitly and the t.Cleanup is a no-op on the second pass.
type encryptedStoreFixture struct {
	dir      string
	mvcc     MVCCStore
	cipher   *encryption.Cipher
	keyID    uint32
	keystore *encryption.Keystore
	closed   bool
}

func (f *encryptedStoreFixture) closeIfOpen(tb testing.TB) {
	tb.Helper()
	if f.closed {
		return
	}
	f.closed = true
	if err := f.mvcc.Close(); err != nil {
		tb.Fatalf("close pebble: %v", err)
	}
}

func (f *encryptedStoreFixture) reopen(tb testing.TB) {
	tb.Helper()
	mvcc, err := NewPebbleStore(f.dir,
		WithEncryption(f.cipher,
			NewCounterNonceFactory(0xABCD, 0x0001),
			func() (uint32, bool) { return f.keyID, true },
		),
	)
	if err != nil {
		tb.Fatalf("reopen NewPebbleStore: %v", err)
	}
	f.mvcc = mvcc
	f.closed = false
}

func newEncryptedStoreFixture(t *testing.T, keyID uint32) *encryptedStoreFixture {
	t.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read DEK: %v", err)
	}
	if err := ks.Set(keyID, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0xABCD, 0x0001),
			func() (uint32, bool) { return keyID, true },
		),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	f := &encryptedStoreFixture{
		dir:      dir,
		mvcc:     mvcc,
		cipher:   c,
		keyID:    keyID,
		keystore: ks,
	}
	t.Cleanup(func() {
		if !f.closed {
			_ = f.mvcc.Close()
		}
	})
	return f
}

// tamperPebbleValue closes the encrypted store, opens the same
// directory directly via Pebble, applies tamperFn to the on-disk
// value at (key, ts), writes it back, and reopens through
// newEncryptedStoreFixture's pattern. The fixture's `mvcc` field is
// replaced so subsequent reads go through the new handle.
//
// The pattern is necessary because Pebble holds an exclusive lock on
// the dir while open; bypassing the public API to plant adversarial
// bytes would otherwise contend with the live store.
func (f *encryptedStoreFixture) tamperPebbleValue(t *testing.T, key []byte, ts uint64, tamperFn func(raw []byte) []byte) {
	t.Helper()
	f.closeIfOpen(t)
	pdb, err := pebble.Open(f.dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open for tamper: %v", err)
	}
	pebbleKey := encodeKey(key, ts)
	raw, closer, err := pdb.Get(pebbleKey)
	if err != nil {
		_ = pdb.Close()
		t.Fatalf("read raw value: %v", err)
	}
	tampered := tamperFn(append([]byte(nil), raw...))
	_ = closer.Close()
	if err := pdb.Set(pebbleKey, tampered, pebble.Sync); err != nil {
		_ = pdb.Close()
		t.Fatalf("write tampered: %v", err)
	}
	if err := pdb.Close(); err != nil {
		t.Fatalf("pebble close: %v", err)
	}
	f.reopen(t)
}

func TestEncryption_PutGet_Roundtrip(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 7)
	ctx := context.Background()
	cases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"short", []byte("k1"), []byte("hello")},
		{"empty value", []byte("k2"), []byte("")},
		{"binary", []byte{0x00, 0xff, 0x10}, []byte{0xde, 0xad, 0xbe, 0xef}},
		{"4 KiB", []byte("k4"), bytes.Repeat([]byte("A"), 4096)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := f.mvcc.PutAt(ctx, tc.key, tc.value, 100, 0); err != nil {
				t.Fatalf("PutAt: %v", err)
			}
			got, err := f.mvcc.GetAt(ctx, tc.key, 100)
			if err != nil {
				t.Fatalf("GetAt: %v", err)
			}
			if !bytes.Equal(got, tc.value) {
				t.Fatalf("GetAt round-trip mismatch: got=%q want=%q", got, tc.value)
			}
		})
	}
}

func TestEncryption_ApplyMutationsRoundtrip(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 11)
	ctx := context.Background()
	muts := []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("a"), Value: []byte("alpha")},
		{Op: OpTypePut, Key: []byte("b"), Value: []byte("beta")},
		{Op: OpTypePut, Key: []byte("c"), Value: bytes.Repeat([]byte("X"), 1024)},
	}
	if err := f.mvcc.ApplyMutations(ctx, muts, nil, 199, 200); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	for _, m := range muts {
		got, err := f.mvcc.GetAt(ctx, m.Key, 200)
		if err != nil {
			t.Fatalf("GetAt %q: %v", m.Key, err)
		}
		if !bytes.Equal(got, m.Value) {
			t.Fatalf("GetAt %q mismatch: got=%q want=%q", m.Key, got, m.Value)
		}
	}
}

// TestEncryption_TombstoneIndependent confirms tombstone writes do
// NOT trip the encryption path (they carry no plaintext) and reads
// of a deleted key surface ErrKeyNotFound regardless of the cipher
// being wired.
func TestEncryption_TombstoneIndependent(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 13)
	ctx := context.Background()
	if err := f.mvcc.PutAt(ctx, []byte("doomed"), []byte("alive"), 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	if err := f.mvcc.DeleteAt(ctx, []byte("doomed"), 200); err != nil {
		t.Fatalf("DeleteAt: %v", err)
	}
	if _, err := f.mvcc.GetAt(ctx, []byte("doomed"), 250); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
	got, err := f.mvcc.GetAt(ctx, []byte("doomed"), 150)
	if err != nil {
		t.Fatalf("GetAt pre-delete: %v", err)
	}
	if !bytes.Equal(got, []byte("alive")) {
		t.Fatalf("pre-delete read mismatch: got=%q want=%q", got, "alive")
	}
}

// newToggleableEncryptedStore returns an MVCCStore wired with an
// encryption cipher whose active-key closure honours the supplied
// activeFlag pointer. Used by the activation/deactivation windows
// test.
func newToggleableEncryptedStore(t *testing.T, keyID uint32, activeFlag *bool) MVCCStore {
	t.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xAA
	if err := ks.Set(keyID, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0x0011, 0x0002),
			func() (uint32, bool) {
				if !*activeFlag {
					return 0, false
				}
				return keyID, true
			},
		),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = mvcc.Close() })
	return mvcc
}

// gatedFixture bundles the Stage 6D-5 gate test store with its
// dir + an idempotent close helper. Pebble panics on double-close,
// so the cleanup MUST track whether the test already closed the
// store before the on-disk peek (rawEncStateAt opens the same
// directory).
type gatedFixture struct {
	mvcc   MVCCStore
	dir    string
	closed bool
}

func (g *gatedFixture) closeIfOpen(tb testing.TB) {
	tb.Helper()
	if g.closed {
		return
	}
	g.closed = true
	if err := g.mvcc.Close(); err != nil {
		tb.Fatalf("close pebble: %v", err)
	}
}

// newGatedEncryptedStore wires the Stage 6D-5 cutover gate
// (WithStorageEnvelopeGate) alongside the existing always-active
// keyID closure. The supplied gateFlag pointer is read by the
// gate on every Put — the test toggles it to exercise the
// pre-cutover (false) vs post-cutover (true) write semantics
// without re-opening the store. Tests use the returned fixture's
// closeIfOpen() before calling rawEncStateAt to release Pebble's
// directory lock.
func newGatedEncryptedStore(t *testing.T, keyID uint32, gateFlag *bool) *gatedFixture {
	t.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xBB
	if err := ks.Set(keyID, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0x0022, 0x0003),
			func() (uint32, bool) { return keyID, true }, // DEK always provisioned
		),
		WithStorageEnvelopeGate(func() bool { return *gateFlag }),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	g := &gatedFixture{mvcc: mvcc, dir: dir}
	t.Cleanup(func() {
		if g.closed {
			return
		}
		// Surface any close error rather than swallowing it: a
		// silent failure here could mask a leaked Pebble handle
		// or a flush bug under a future code change (gemini-code-
		// assist medium on PR809).
		if err := g.mvcc.Close(); err != nil {
			t.Errorf("cleanup close pebble: %v", err)
		}
	})
	return g
}

// rawEncStateAt opens Pebble directly at dir (the caller MUST
// have closed the MVCCStore at that path first; Pebble holds an
// exclusive lock) and reads the on-disk encryption_state bits of
// (key, ts). Lets the gate tests prove the wire-level effect of
// the toggle rather than just the round-trip behaviour.
func rawEncStateAt(t *testing.T, dir string, key []byte, ts uint64) byte {
	t.Helper()
	pdb, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	defer func() {
		if cerr := pdb.Close(); cerr != nil {
			t.Errorf("pdb.Close: %v", cerr)
		}
	}()
	raw, closer, err := pdb.Get(encodeKey(key, ts))
	if err != nil {
		t.Fatalf("pdb.Get %q@%d: %v", key, ts, err)
	}
	defer func() {
		if cerr := closer.Close(); cerr != nil {
			t.Errorf("closer.Close: %v", cerr)
		}
	}()
	if len(raw) == 0 {
		t.Fatalf("empty raw value at %q@%d", key, ts)
	}
	return (raw[0] & encStateMask) >> encStateShift
}

// mustGet calls GetAt and asserts the value matches want, surfacing
// any error via t.Fatalf so the caller test stays linear.
func mustGet(t *testing.T, mvcc MVCCStore, key []byte, ts uint64, want string) {
	t.Helper()
	got, err := mvcc.GetAt(context.Background(), key, ts)
	if err != nil {
		t.Fatalf("GetAt %q@%d: %v", key, ts, err)
	}
	if string(got) != want {
		t.Fatalf("GetAt %q@%d: got=%q want=%q", key, ts, got, want)
	}
}

// TestEncryption_InactiveKeyWritesCleartext exercises the §7.1
// rollout's "cipher wired but DEK not active yet" window: PutAt
// writes cleartext (encryption_state = 0b00) when the active-key
// closure returns ok=false. A later activation must not break reads
// of those cleartext entries — the read path looks at the per-value
// encryption_state, not the global active-key state.
func TestEncryption_InactiveKeyWritesCleartext(t *testing.T) {
	t.Parallel()
	var active bool
	mvcc := newToggleableEncryptedStore(t, 42, &active)
	ctx := context.Background()

	// active=false → cleartext write
	if err := mvcc.PutAt(ctx, []byte("legacy"), []byte("plain"), 100, 0); err != nil {
		t.Fatalf("PutAt before activation: %v", err)
	}
	active = true
	if err := mvcc.PutAt(ctx, []byte("modern"), []byte("encrypted"), 200, 0); err != nil {
		t.Fatalf("PutAt after activation: %v", err)
	}
	mustGet(t, mvcc, []byte("legacy"), 150, "plain")
	mustGet(t, mvcc, []byte("modern"), 250, "encrypted")
	// Deactivating must not break reads of the already-encrypted entry.
	active = false
	mustGet(t, mvcc, []byte("modern"), 250, "encrypted")
}

// TestStorageEnvelopeGate_PreCutoverForcesCleartext pins the
// Stage 6D-5 §6.2 toggle: even with a cipher and active DEK
// wired, a `StorageEnvelopeActive == false` gate suppresses the
// envelope emit and the on-disk encryption_state stays 0b00
// (cleartext). This is the operator-inert post-bootstrap /
// pre-cutover posture from the §7.1 Phase 0 window.
func TestStorageEnvelopeGate_PreCutoverForcesCleartext(t *testing.T) {
	t.Parallel()
	var gate bool // false == pre-cutover
	g := newGatedEncryptedStore(t, 51, &gate)
	ctx := context.Background()

	if err := g.mvcc.PutAt(ctx, []byte("pre"), []byte("v-pre"), 100, 0); err != nil {
		t.Fatalf("PutAt pre-cutover: %v", err)
	}
	mustGet(t, g.mvcc, []byte("pre"), 150, "v-pre")
	// Close the store so Pebble releases its directory lock; we
	// reach in to confirm the on-disk encryption_state bits.
	g.closeIfOpen(t)
	if got := rawEncStateAt(t, g.dir, []byte("pre"), 100); got != encStateCleartext {
		t.Errorf("pre-cutover on-disk encState = %#x, want %#x (cleartext)",
			got, encStateCleartext)
	}
}

// TestStorageEnvelopeGate_PostCutoverEmitsEnvelope is the
// positive-control: with the gate returning true, the existing
// envelope emit fires normally. Without this test a regression
// that always returned cleartext from encryptForKey would not
// be caught by the negative test above.
func TestStorageEnvelopeGate_PostCutoverEmitsEnvelope(t *testing.T) {
	t.Parallel()
	gate := true // post-cutover
	g := newGatedEncryptedStore(t, 52, &gate)
	ctx := context.Background()

	if err := g.mvcc.PutAt(ctx, []byte("post"), []byte("v-post"), 100, 0); err != nil {
		t.Fatalf("PutAt post-cutover: %v", err)
	}
	mustGet(t, g.mvcc, []byte("post"), 150, "v-post")
	g.closeIfOpen(t)
	if got := rawEncStateAt(t, g.dir, []byte("post"), 100); got != encStateEncrypted {
		t.Errorf("post-cutover on-disk encState = %#x, want %#x (encrypted)",
			got, encStateEncrypted)
	}
}

// TestStorageEnvelopeGate_FlipMidLife exercises the §7.1 Phase 0
// → Phase 1 transition: writes before the gate flips are cleartext,
// writes after are §4.1 envelope, AND reads of the pre-cutover
// versions continue to work after the flip — proving the gate is
// write-only and does NOT affect the cipher-driven read dispatch.
func TestStorageEnvelopeGate_FlipMidLife(t *testing.T) {
	t.Parallel()
	var gate bool
	g := newGatedEncryptedStore(t, 53, &gate)
	ctx := context.Background()

	if err := g.mvcc.PutAt(ctx, []byte("legacy"), []byte("v-legacy"), 100, 0); err != nil {
		t.Fatalf("PutAt pre-cutover: %v", err)
	}
	gate = true // §7.1 Phase 1 cutover fires
	if err := g.mvcc.PutAt(ctx, []byte("modern"), []byte("v-modern"), 200, 0); err != nil {
		t.Fatalf("PutAt post-cutover: %v", err)
	}
	// Both reads succeed live — the gate is write-only; reads
	// dispatch on each version's stored encryption_state.
	mustGet(t, g.mvcc, []byte("legacy"), 150, "v-legacy")
	mustGet(t, g.mvcc, []byte("modern"), 250, "v-modern")
	// And the on-disk encryption_state bits actually differ.
	g.closeIfOpen(t)
	if got := rawEncStateAt(t, g.dir, []byte("legacy"), 100); got != encStateCleartext {
		t.Errorf("legacy on-disk encState = %#x, want %#x (cleartext)",
			got, encStateCleartext)
	}
	if got := rawEncStateAt(t, g.dir, []byte("modern"), 200); got != encStateEncrypted {
		t.Errorf("modern on-disk encState = %#x, want %#x (encrypted)",
			got, encStateEncrypted)
	}
}

// TestStorageEnvelopeGate_NilGatePreservesLegacyBehavior pins the
// backward-compat posture: a store wired with WithEncryption but
// WITHOUT WithStorageEnvelopeGate continues to emit envelopes as
// soon as a DEK is active, exactly like the pre-6D-5 codepath.
// Existing test fixtures and pre-6D-6 production deployments
// (which have not opted into the cutover semantics yet) keep
// working unchanged.
//
// The negative test below uses the unmodified
// newEncryptedStoreFixture which calls WithEncryption with no
// gate. If a regression hard-required the gate to be set, every
// pre-existing encryption test would fail.
func TestStorageEnvelopeGate_NilGatePreservesLegacyBehavior(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 54)
	ctx := context.Background()
	if err := f.mvcc.PutAt(ctx, []byte("legacy-mode"), []byte("encrypted"), 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	mustGet(t, f.mvcc, []byte("legacy-mode"), 150, "encrypted")
	f.closeIfOpen(t)
	if got := rawEncStateAt(t, f.dir, []byte("legacy-mode"), 100); got != encStateEncrypted {
		t.Errorf("nil-gate on-disk encState = %#x, want %#x (encrypted; legacy posture)",
			got, encStateEncrypted)
	}
}

// TestStorageEnvelopeGate_NilArgumentIsNoOp pins the
// WithStorageEnvelopeGate(nil) refusal: passing nil leaves the
// store in its pre-call gate posture (nil internal field). A
// regression that accepted nil and assigned it as the gate would
// nil-deref on the first Put.
func TestStorageEnvelopeGate_NilArgumentIsNoOp(t *testing.T) {
	t.Parallel()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	dek[0] = 0xCC
	if err := ks.Set(60, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	dir := filepath.Join(t.TempDir(), "pebble")
	mvcc, err := NewPebbleStore(dir,
		WithEncryption(c,
			NewCounterNonceFactory(0x0033, 0x0004),
			func() (uint32, bool) { return 60, true },
		),
		WithStorageEnvelopeGate(nil), // explicit nil → no-op
	)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	// closed tracks whether the test body has already closed the
	// store before the on-disk peek; the cleanup must surface
	// any unexpected close error but skip the double-close that
	// would otherwise panic inside Pebble (gemini-code-assist
	// medium + coderabbit minor on PR809).
	var closed bool
	t.Cleanup(func() {
		if closed {
			return
		}
		if cerr := mvcc.Close(); cerr != nil {
			t.Errorf("cleanup close pebble: %v", cerr)
		}
	})
	ctx := context.Background()
	// Without a gate, the store should still emit envelopes
	// (legacy posture). A nil-deref bug surfaces as a panic here.
	if err := mvcc.PutAt(ctx, []byte("k"), []byte("v"), 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	mustGet(t, mvcc, []byte("k"), 150, "v")
	// Coderabbit minor: a round-trip pass alone does not prove
	// the legacy-posture write took the envelope path — a
	// regression that always wrote cleartext would still
	// round-trip cleanly. Close and peek the on-disk
	// encryption_state bits to pin the wire-level effect.
	closed = true
	if cerr := mvcc.Close(); cerr != nil {
		t.Fatalf("close pebble: %v", cerr)
	}
	if got := rawEncStateAt(t, dir, []byte("k"), 100); got != encStateEncrypted {
		t.Errorf("nil-gate on-disk encState = %#x, want %#x (encrypted; legacy posture)",
			got, encStateEncrypted)
	}
}

// TestEncryption_AADRecordBinding is the §4.1 case-2/3 regression:
// copying a valid encrypted value from one (key, ts) slot into
// another must NOT verify under the target — the AAD includes the
// encoded Pebble key, so Decrypt returns ErrEncryptedReadIntegrity.
func TestEncryption_AADRecordBinding(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 19)
	ctx := context.Background()

	if err := f.mvcc.PutAt(ctx, []byte("a"), []byte("secret-a"), 100, 0); err != nil {
		t.Fatalf("PutAt a: %v", err)
	}
	if err := f.mvcc.PutAt(ctx, []byte("b"), []byte("secret-b"), 100, 0); err != nil {
		t.Fatalf("PutAt b: %v", err)
	}

	// Snapshot a's raw on-disk bytes, then overwrite b's slot with
	// them. Done via the close-tamper-reopen helper so the fixture's
	// idempotent close keeps the t.Cleanup safe.
	f.closeIfOpen(t)
	pdb, err := pebble.Open(f.dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	rawA, closer, err := pdb.Get(encodeKey([]byte("a"), 100))
	if err != nil {
		_ = pdb.Close()
		t.Fatalf("read raw a: %v", err)
	}
	rawACopy := append([]byte(nil), rawA...)
	_ = closer.Close()
	if err := pdb.Set(encodeKey([]byte("b"), 100), rawACopy, pebble.Sync); err != nil {
		_ = pdb.Close()
		t.Fatalf("write tampered b: %v", err)
	}
	if err := pdb.Close(); err != nil {
		t.Fatalf("pebble close: %v", err)
	}
	f.reopen(t)

	if _, err := f.mvcc.GetAt(ctx, []byte("b"), 100); !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("cut-and-paste ciphertext should fail integrity, got %v", err)
	}
}

// TestEncryption_TagTamper flips one byte of the GCM tag and confirms
// reads surface ErrEncryptedReadIntegrity.
func TestEncryption_TagTamper(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 23)
	ctx := context.Background()
	if err := f.mvcc.PutAt(ctx, []byte("tamper"), []byte("payload"), 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	f.tamperPebbleValue(t, []byte("tamper"), 100, func(raw []byte) []byte {
		raw[len(raw)-1] ^= 0xff
		return raw
	})
	if _, err := f.mvcc.GetAt(ctx, []byte("tamper"), 100); !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("tag tamper should fail integrity, got %v", err)
	}
}

// TestEncryption_ValueHeaderTamperRejected is the PR742 codex P1
// regression: the value-header (tombstone bit + encryption_state +
// expireAt) is bound into the storage envelope's AAD so a disk
// attacker who flips any of those fields fails GCM verification
// and surfaces ErrEncryptedReadIntegrity, NOT a silent
// ErrKeyNotFound or expired-skip. The original AAD only bound the
// envelope header + Pebble key, leaving these three header fields
// as a tamper bypass.
func TestEncryption_ValueHeaderTamperRejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(raw []byte) []byte
		summary string
	}{
		{
			name:    "tombstone bit flipped",
			summary: "would otherwise force silent ErrKeyNotFound on a live encrypted record",
			mutate: func(raw []byte) []byte {
				raw[0] |= 0b0000_0001 // set tombstone bit
				return raw
			},
		},
		{
			name:    "expireAt lowered to past",
			summary: "would otherwise force a silent expired-skip on a live encrypted record",
			mutate: func(raw []byte) []byte {
				// Overwrite the 8-byte expireAt with a past timestamp;
				// before the AAD fix this was a free attack vector.
				past := []byte{0x01, 0, 0, 0, 0, 0, 0, 0}
				copy(raw[1:1+timestampSize], past)
				return raw
			},
		},
		{
			name:    "expireAt advanced",
			summary: "asymmetric — but any change must still fail closed",
			mutate: func(raw []byte) []byte {
				future := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
				copy(raw[1:1+timestampSize], future)
				return raw
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEncryptedStoreFixture(t, 1009)
			ctx := context.Background()
			// Use a future-but-finite expireAt so a "lower to past"
			// mutation has somewhere to go AND so the original entry
			// is live (expireAt > read ts).
			const writeTS uint64 = 100
			const readTS uint64 = 200
			const liveExpireAt uint64 = 1_000_000
			if err := f.mvcc.PutAt(ctx, []byte("vh"), []byte("payload"), writeTS, liveExpireAt); err != nil {
				t.Fatalf("PutAt: %v", err)
			}
			f.tamperPebbleValue(t, []byte("vh"), writeTS, tc.mutate)
			_, err := f.mvcc.GetAt(ctx, []byte("vh"), readTS)
			if !errors.Is(err, ErrEncryptedReadIntegrity) {
				t.Fatalf("%s: expected ErrEncryptedReadIntegrity (%s), got %v",
					tc.name, tc.summary, err)
			}
		})
	}
}

func TestEncryptionScanKeysAtHeaderTamperRejected(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 1059)
	ctx := context.Background()
	const writeTS uint64 = 100
	const readTS uint64 = 200
	if err := f.mvcc.PutAt(ctx, []byte("scan-key"), []byte("payload"), writeTS, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	keys, err := f.mvcc.ScanKeysAt(ctx, []byte("scan-"), []byte("scan."), 10, readTS)
	if err != nil {
		t.Fatalf("ScanKeysAt before tamper: %v", err)
	}
	if len(keys) != 1 || !bytes.Equal(keys[0], []byte("scan-key")) {
		t.Fatalf("ScanKeysAt before tamper = %q, want scan-key", keys)
	}

	f.tamperPebbleValue(t, []byte("scan-key"), writeTS, func(raw []byte) []byte {
		raw[0] |= tombstoneMask
		return raw
	})
	_, err = f.mvcc.ScanKeysAt(ctx, []byte("scan-"), []byte("scan."), 10, readTS)
	if !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("ScanKeysAt over a tampered encrypted entry should fail integrity, got %v", err)
	}
}

// TestEncryption_DeletePrefixHeaderTamperRejected covers the PR742
// claude[bot] round-5 follow-up: isVisibleLiveKey is the write-side
// counterpart to readVisibleVersion / processFoundValue (read paths
// fixed in rounds 3–5). Without authenticating the value-header,
// a disk attacker who flips the tombstone bit on an encrypted entry
// would cause DeletePrefixAt to skip writing the deletion tombstone,
// silently leaving the key alive after the prefix delete — a
// write-side integrity bypass that survives across restarts.
//
// The fix runs decryptForKey inside isVisibleLiveKey; this test
// pins the contract that scanDeletePrefix surfaces the integrity
// error rather than no-oping the tombstone.
func TestEncryption_DeletePrefixHeaderTamperRejected(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 41)
	ctx := context.Background()
	const writeTS uint64 = 100
	const deleteTS uint64 = 200
	if err := f.mvcc.PutAt(ctx, []byte("doomed/k1"), []byte("victim"), writeTS, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	// Flip the tombstone bit on the encrypted entry's value header
	// directly on disk. Pre-fix, scanDeletePrefix would observe the
	// flipped sv.Tombstone and skip writing a tombstone for this key.
	f.tamperPebbleValue(t, []byte("doomed/k1"), writeTS, func(raw []byte) []byte {
		raw[0] |= tombstoneMask
		return raw
	})
	err := f.mvcc.DeletePrefixAt(ctx, []byte("doomed/"), nil, deleteTS)
	if !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("DeletePrefixAt over a tampered encrypted entry should fail integrity, got %v", err)
	}
}

// TestEncryption_RebadgeAttackRejected covers the PR742 codex P1
// rebadge attack family: a disk attacker who flips encryption_state
// from 0b01 to 0b00 leaves the envelope bytes in place but tells
// the read path to skip decryption. Without the cleartext-branch
// guard in decryptForKey, the caller would receive raw envelope
// bytes as "plaintext" — a fail-open integrity bypass.
//
// Round-3 caught the simple flip; round-4 caught the variant where
// the attacker ALSO modifies the embedded key_id bytes to any
// unloaded value. The strengthened guard rejects whenever
// DecodeEnvelope parses the body — independent of whether the
// embedded key_id is currently loaded.
func TestEncryption_RebadgeAttackRejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(raw []byte) []byte
		writeTS uint64
	}{
		{
			name:    "encState flipped, key_id intact",
			writeTS: 314159,
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask
				return raw
			},
		},
		{
			name:    "encState flipped AND key_id rewritten to unloaded",
			writeTS: 271828,
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask
				// envelope key_id is at byte offset valueHeaderSize+2
				// (skip flags(1)+expireAt(8) + version(1)+flag(1)).
				kidOffset := valueHeaderSize + 2
				// Set to 0xFFFFFFFF, an id that the test fixture has
				// not loaded. Pre-round-4 guard returned nil here.
				raw[kidOffset+0] = 0xff
				raw[kidOffset+1] = 0xff
				raw[kidOffset+2] = 0xff
				raw[kidOffset+3] = 0xff
				return raw
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEncryptedStoreFixture(t, 31)
			ctx := context.Background()
			if err := f.mvcc.PutAt(ctx, []byte("rebadge"), []byte("classified"), tc.writeTS, 0); err != nil {
				t.Fatalf("PutAt: %v", err)
			}
			f.tamperPebbleValue(t, []byte("rebadge"), tc.writeTS, tc.mutate)
			_, err := f.mvcc.GetAt(ctx, []byte("rebadge"), tc.writeTS)
			if !errors.Is(err, ErrEncryptedReadIntegrity) {
				t.Fatalf("rebadge attempt should fail integrity, got %v", err)
			}
		})
	}
}

// TestEncryption_SnapshotRestoreAtMaxValueSize covers the PR742
// codex P1 round-8 finding: validateValueSize accepts a plaintext
// up to maxSnapshotValueSize, but encryptForKey adds 34 bytes
// (EnvelopeOverhead) on the storage envelope, and the restore
// path's per-entry cap (`maxSnapshotValueSize + valueHeaderSize`)
// would reject the encrypted body — making it possible to persist
// data that cannot be recovered via snapshot. The fix raises the
// restore cap by EnvelopeOverhead so a plaintext written at the
// exact maxSnapshotValueSize round-trips through Pebble snapshot
// save/restore.
func TestEncryption_SnapshotRestoreAtMaxValueSize(t *testing.T) {
	// NOT t.Parallel: this test mutates the package-level
	// maxSnapshotValueSize var, which other tests read; running it
	// alongside parallel tests trips -race. Keeping it serial is
	// the same convention the existing snapshot suite uses for the
	// same var.
	prev := maxSnapshotValueSize
	maxSnapshotValueSize = 4096
	t.Cleanup(func() { maxSnapshotValueSize = prev })

	f := newEncryptedStoreFixture(t, 71)
	ctx := context.Background()

	// Write a plaintext exactly at the (shrunk) maxSnapshotValueSize.
	value := bytes.Repeat([]byte{0xA5}, maxSnapshotValueSize)
	if err := f.mvcc.PutAt(ctx, []byte("max"), value, 100, 0); err != nil {
		t.Fatalf("PutAt at max value size: %v", err)
	}
	// Capture a snapshot before tearing the source store down.
	snap, err := f.mvcc.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snap.Close()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		t.Fatalf("Snapshot.WriteTo: %v", err)
	}

	// Restore into a fresh store (unencrypted is fine — Pebble snapshots
	// ship raw bytes and the encrypted envelope is preserved verbatim;
	// we are only testing the restore size cap here, not decrypt).
	dstDir := filepath.Join(t.TempDir(), "restore")
	dst, err := NewPebbleStore(dstDir,
		WithEncryption(f.cipher,
			NewCounterNonceFactory(0xABCD, 0x0001),
			func() (uint32, bool) { return f.keyID, true },
		),
	)
	if err != nil {
		t.Fatalf("NewPebbleStore restore-target: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	if err := dst.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Restore at max value size + envelope overhead: %v", err)
	}
	got, err := dst.GetAt(ctx, []byte("max"), 100)
	if err != nil {
		t.Fatalf("GetAt after restore: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("restored value mismatch: len=%d want=%d", len(got), len(value))
	}
}

// TestEncryption_RebadgeAttackEnvelopeHeaderCorruption covers the
// PR742 codex P1 round-9 finding: a disk attacker who flips
// encryption_state to cleartext AND corrupts the envelope's version
// (or flag) byte forces DecodeEnvelope to fail, which the previous
// guard treated as "legitimate cleartext". The round-9 fix bypasses
// DecodeEnvelope and slices the body at fixed offsets, trial-
// decrypting with canonical (version=0x01, flag=0) so a corrupted
// version or flag byte no longer ducks the integrity check.
func TestEncryption_RebadgeAttackEnvelopeHeaderCorruption(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(raw []byte) []byte
	}{
		{
			name: "encState + envelope version byte corrupted",
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask // clear encState bits → cleartext
				// envelope version sits at the start of the body, after
				// the 9-byte value header.
				raw[valueHeaderSize] = 0x07 // arbitrary non-0x01
				return raw
			},
		},
		{
			name: "encState + envelope flag byte corrupted",
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask
				raw[valueHeaderSize+1] = 0xff // flag canonical = 0
				return raw
			},
		},
		{
			name: "encState + version AND flag corrupted",
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask
				raw[valueHeaderSize] = 0x42
				raw[valueHeaderSize+1] = 0x99
				return raw
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEncryptedStoreFixture(t, 73)
			ctx := context.Background()
			const writeTS uint64 = 500001
			if err := f.mvcc.PutAt(ctx, []byte("hdr"), []byte("payload"), writeTS, 0); err != nil {
				t.Fatalf("PutAt: %v", err)
			}
			f.tamperPebbleValue(t, []byte("hdr"), writeTS, tc.mutate)
			_, err := f.mvcc.GetAt(ctx, []byte("hdr"), writeTS)
			if !errors.Is(err, ErrEncryptedReadIntegrity) {
				t.Fatalf("envelope header corruption rebadge should fail integrity, got %v", err)
			}
		})
	}
}

// TestEncryption_RebadgeAttackCombinedHeaderFlips covers the PR742
// codex P1 round-7 finding: a disk attacker who flips
// encryption_state AND simultaneously modifies tombstone or expireAt
// would otherwise bypass the rebadge guard because the trial-decrypt
// AAD reconstructed from the on-disk (tampered) header bytes no
// longer matches the encrypt-time AAD. The fix canonicalises
// tombstone to false in the trial AAD (encrypt path always writes
// tombstone=false) and enumerates expireAt candidates ({on-disk, 0})
// to cover the common no-TTL case.
//
// Residual: encState + expireAt flip when the original expireAt was
// a non-zero value the attacker also rewrites. Stage 8's
// authenticated MVCC metadata bit closes that deterministically.
func TestEncryption_RebadgeAttackCombinedHeaderFlips(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mutate  func(raw []byte) []byte
		writeTS uint64
		// origExpireAt is what we passed to PutAt; the trial guard
		// must catch the attack regardless of what the attacker
		// modifies on top of the encState flip.
		origExpireAt uint64
	}{
		{
			name:         "encState + tombstone flipped (no TTL)",
			writeTS:      400001,
			origExpireAt: 0,
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask // clear bits 1-2
				raw[0] |= tombstoneMask // set bit 0
				return raw
			},
		},
		{
			name:         "encState + expireAt rewritten to past (no TTL)",
			writeTS:      400002,
			origExpireAt: 0,
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask
				// rewrite expireAt to a small past value
				past := []byte{0x01, 0, 0, 0, 0, 0, 0, 0}
				copy(raw[1:1+timestampSize], past)
				return raw
			},
		},
		{
			name:         "encState + tombstone + expireAt all flipped (no TTL)",
			writeTS:      400003,
			origExpireAt: 0,
			mutate: func(raw []byte) []byte {
				raw[0] &^= encStateMask
				raw[0] |= tombstoneMask
				future := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
				copy(raw[1:1+timestampSize], future)
				return raw
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEncryptedStoreFixture(t, 67)
			ctx := context.Background()
			if err := f.mvcc.PutAt(ctx, []byte("combined"), []byte("payload"), tc.writeTS, tc.origExpireAt); err != nil {
				t.Fatalf("PutAt: %v", err)
			}
			f.tamperPebbleValue(t, []byte("combined"), tc.writeTS, tc.mutate)
			_, err := f.mvcc.GetAt(ctx, []byte("combined"), tc.writeTS)
			if !errors.Is(err, ErrEncryptedReadIntegrity) {
				t.Fatalf("combined-flip rebadge attempt should fail integrity, got %v", err)
			}
		})
	}
}

// TestEncryption_RebadgeGuardAllowsLegitimateEnvelopeShapedCleartext
// is the PR742 codex P1 round-6 regression: round-5's "reject any
// envelope-parseable body" guard turned legitimate cleartext rows
// that coincidentally start with 0x01 and pass DecodeEnvelope's
// length / version / flag / nonce checks into deterministic read
// failures. Round-7 replaced the shape-only check with an actual
// AEAD trial decrypt, so a body that does not verify under any
// loaded DEK is allowed through as cleartext.
//
// We construct a cleartext payload whose bytes are envelope-shaped:
// 0x01 version, 0x00 flag, an arbitrary 4-byte key_id, 12 random
// nonce bytes, and 16+ bytes of "ciphertext+tag" filler. Pre-fix
// the read would error; post-fix the body is returned unchanged.
func TestEncryption_RebadgeGuardAllowsLegitimateEnvelopeShapedCleartext(t *testing.T) {
	t.Parallel()
	// Build the store with cipher wired but the active-key flag
	// pointing at "no DEK" so PutAt writes cleartext. This models
	// the §7.1 Phase 0 / pre-cutover window where legacy data
	// coexists with a configured cipher.
	var active bool
	mvcc := newToggleableEncryptedStore(t, 71, &active)
	ctx := context.Background()

	envelopeShaped := make([]byte, 64)
	envelopeShaped[0] = 0x01 // EnvelopeVersionV1
	// flag stays 0; key_id = 0xCAFEBABE; nonce + body filler are arbitrary.
	envelopeShaped[2] = 0xCA
	envelopeShaped[3] = 0xFE
	envelopeShaped[4] = 0xBA
	envelopeShaped[5] = 0xBE
	for i := 6; i < len(envelopeShaped); i++ {
		envelopeShaped[i] = byte(i)
	}

	if err := mvcc.PutAt(ctx, []byte("legit"), envelopeShaped, 100, 0); err != nil {
		t.Fatalf("PutAt cleartext: %v", err)
	}
	// Activate encryption AFTER the cleartext write — read must
	// still surface the original bytes verbatim.
	active = true
	got, err := mvcc.GetAt(ctx, []byte("legit"), 100)
	if err != nil {
		t.Fatalf("GetAt envelope-shaped cleartext: %v (round-5 regression)", err)
	}
	if !bytes.Equal(got, envelopeShaped) {
		t.Fatalf("round-trip mismatch: got %x, want %x", got, envelopeShaped)
	}
}

// TestEncryption_EmptyValueExistsAt is the PR742 codex P1 round-4
// regression for empty-plaintext semantics: AES-GCM Open returns a
// nil dst for zero-length plaintext, and the upstream ExistsAt
// distinguishes "key absent" from "key present with empty value"
// via val != nil. decryptForKey now normalizes the nil-on-empty
// case to []byte{} so a Put of an empty value followed by ExistsAt
// returns true.
func TestEncryption_EmptyValueExistsAt(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 37)
	ctx := context.Background()
	if err := f.mvcc.PutAt(ctx, []byte("empty"), []byte{}, 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	got, err := f.mvcc.GetAt(ctx, []byte("empty"), 100)
	if err != nil {
		t.Fatalf("GetAt: %v", err)
	}
	if got == nil {
		t.Fatal("GetAt returned nil for an empty stored value (regresses ExistsAt)")
	}
	if len(got) != 0 {
		t.Fatalf("GetAt returned %d bytes, want 0", len(got))
	}
	exists, err := f.mvcc.ExistsAt(ctx, []byte("empty"), 100)
	if err != nil {
		t.Fatalf("ExistsAt: %v", err)
	}
	if !exists {
		t.Fatal("ExistsAt returned false for a key with empty stored value")
	}
}

// TestEncryption_ReservedEncStateRejected is the §7.1 trip-wire test:
// a value-header byte carrying encryption_state=0b10 (or 0b11) is
// rejected by decodeValue with ErrEncryptedValueReservedState. An
// older binary must NOT silently treat a future-format encrypted
// entry as cleartext bytes.
func TestEncryption_ReservedEncStateRejected(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 29)
	ctx := context.Background()
	if err := f.mvcc.PutAt(ctx, []byte("reserved"), []byte("body"), 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	f.tamperPebbleValue(t, []byte("reserved"), 100, func(raw []byte) []byte {
		// Flip encryption_state from 0b01 to 0b10 (bits 1-2) without
		// touching tombstone (bit 0) or reserved bits (3-7).
		raw[0] = (raw[0] & 0b1111_1001) | 0b0000_0100
		return raw
	})
	_, err := f.mvcc.GetAt(ctx, []byte("reserved"), 100)
	if !errors.Is(err, ErrEncryptedValueReservedState) {
		t.Fatalf("reserved encState should be rejected, got %v", err)
	}
}

// TestEncryption_HeaderEncodingPin pins the value-header packing so
// future refactors that re-pack the bits accidentally cannot land.
// The §4.1 contract is load-bearing for every persisted encrypted
// entry: tombstone in bit 0, encryption_state in bits 1-2, reserved
// elsewhere.
func TestEncryption_HeaderEncodingPin(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		tombstone bool
		encState  byte
		want      byte
	}{
		{"cleartext live", false, encStateCleartext, 0b0000_0000},
		{"cleartext tombstone", true, encStateCleartext, 0b0000_0001},
		{"encrypted live", false, encStateEncrypted, 0b0000_0010},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := encodeValue(nil, tc.tombstone, 0, tc.encState)
			if buf[0] != tc.want {
				t.Fatalf("flags byte = %#08b, want %#08b", buf[0], tc.want)
			}
			sv, err := decodeValue(buf)
			if err != nil {
				t.Fatalf("decodeValue: %v", err)
			}
			if sv.Tombstone != tc.tombstone || sv.EncState != tc.encState {
				t.Fatalf("decode mismatch: got tomb=%v enc=%#x, want tomb=%v enc=%#x",
					sv.Tombstone, sv.EncState, tc.tombstone, tc.encState)
			}
		})
	}
}
