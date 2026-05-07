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

// TestEncryption_RebadgeAttackRejected covers the PR742 codex P1
// (round-3) finding: a disk attacker who flips encryption_state from
// 0b01 to 0b00 leaves the envelope bytes in place but tells the read
// path to skip decryption. Without the cleartext-branch guard in
// decryptForKey, the caller would receive raw envelope bytes as
// "plaintext" — a fail-open integrity bypass.
//
// The guard parses the body as an envelope and consults
// Cipher.HasKey for the embedded key_id; when both match (the
// realistic post-flip shape), the read fails closed with
// ErrEncryptedReadIntegrity.
func TestEncryption_RebadgeAttackRejected(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 31)
	ctx := context.Background()
	const writeTS uint64 = 314159 // distinguishable from the other tamper tests' ts=100
	if err := f.mvcc.PutAt(ctx, []byte("rebadge"), []byte("classified"), writeTS, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	f.tamperPebbleValue(t, []byte("rebadge"), writeTS, func(raw []byte) []byte {
		// Flip encryption_state from 0b01 to 0b00 (clear bits 1-2)
		// without touching tombstone bit or expireAt.
		raw[0] &^= encStateMask
		return raw
	})
	_, err := f.mvcc.GetAt(ctx, []byte("rebadge"), writeTS)
	if !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("rebadge attempt should fail integrity, got %v", err)
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
