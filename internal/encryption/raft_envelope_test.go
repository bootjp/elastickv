package encryption_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

// raftFixture wires a freshly-keyed Cipher with one DEK at testKeyID
// — the Stage-3 raft envelope tests don't need to model multiple DEKs;
// rotation / retire scenarios are covered by the storage-envelope
// suite that exercises the same Cipher / Keystore.
func raftFixture(t *testing.T) (*encryption.Cipher, uint32) {
	t.Helper()
	ks, kid := newKeystoreWithKey(t)
	return mustCipher(t, ks), kid
}

func TestRaftEnvelope_RoundTrip(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", []byte{}},
		{"short", []byte("op=put key=k1 v=hello")},
		{"binary", []byte{0x00, 0xff, 0x10, 0x42, 0xde, 0xad}},
		{"4 KiB", bytes.Repeat([]byte("X"), 4096)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nonce := newRandomNonce(t)
			encoded, err := encryption.WrapRaftPayload(c, kid, nonce, tc.payload)
			if err != nil {
				t.Fatalf("Wrap: %v", err)
			}
			if got, want := len(encoded), len(tc.payload)+encryption.EnvelopeOverhead; got != want {
				t.Fatalf("encoded len=%d, want %d", got, want)
			}
			plain, err := encryption.UnwrapRaftPayload(c, encoded)
			if err != nil {
				t.Fatalf("Unwrap: %v", err)
			}
			if !bytes.Equal(plain, tc.payload) {
				t.Fatalf("plaintext mismatch: got %x, want %x", plain, tc.payload)
			}
		})
	}
}

// TestRaftEnvelope_DeterministicNonce confirms that the same
// (keyID, nonce, payload) triple produces the same encoded bytes.
// The §4.2 deterministic nonce factory relies on this property to
// reproduce ciphertexts deterministically across replays.
func TestRaftEnvelope_DeterministicNonce(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	nonce := newRandomNonce(t)
	payload := []byte("repeatable payload")
	a, err := encryption.WrapRaftPayload(c, kid, nonce, payload)
	if err != nil {
		t.Fatalf("Wrap A: %v", err)
	}
	b, err := encryption.WrapRaftPayload(c, kid, nonce, payload)
	if err != nil {
		t.Fatalf("Wrap B: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("deterministic-nonce wrap produced different bytes for identical inputs")
	}
}

// TestRaftEnvelope_RejectsTagTamper flips a byte inside the GCM tag
// region (last 16 bytes) and confirms Unwrap surfaces ErrIntegrity.
// Mirrors the §4.1 storage-envelope tag-tamper test.
func TestRaftEnvelope_RejectsTagTamper(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	encoded, err := encryption.WrapRaftPayload(c, kid, newRandomNonce(t), []byte("payload"))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	encoded[len(encoded)-1] ^= 0xff
	_, err = encryption.UnwrapRaftPayload(c, encoded)
	if !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected ErrIntegrity, got %v", err)
	}
}

// TestRaftEnvelope_RejectsKeyIDTamper flips a key_id byte inside the
// envelope header. The key_id participates in the AAD via
// BuildRaftAAD, so the flip changes the AAD on Unwrap and GCM
// rejects the tag.
func TestRaftEnvelope_RejectsKeyIDTamper(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	encoded, err := encryption.WrapRaftPayload(c, kid, newRandomNonce(t), []byte("payload"))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	// keyID lives at offset 2..5 (version=0, flag=1, keyID=2-5).
	encoded[2] ^= 0x01
	_, err = encryption.UnwrapRaftPayload(c, encoded)
	if !errors.Is(err, encryption.ErrIntegrity) && !errors.Is(err, encryption.ErrUnknownKeyID) {
		// Either outcome is acceptable: ErrIntegrity if the tampered
		// key_id remains in the keystore (impossible with a single
		// loaded DEK), ErrUnknownKeyID if the tampered key_id is no
		// longer loaded. With a single DEK loaded, a flipped low bit
		// almost always lands on an unknown key_id.
		t.Fatalf("expected ErrIntegrity or ErrUnknownKeyID, got %v", err)
	}
}

// TestRaftEnvelope_RejectsStorageEnvelope confirms a §4.1 storage
// envelope (whose AAD includes the value-header bytes and pebble
// key, but NOT the 'R' purpose byte) fails GCM verification when
// fed to UnwrapRaftPayload. This is the layer-confusion defence
// design §4.2 calls out: a storage ciphertext replayed into the
// raft path must not silently decrypt.
func TestRaftEnvelope_RejectsStorageEnvelope(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	nonce := newRandomNonce(t)
	// Build a storage envelope by hand: AAD = HeaderAADBytes ‖ value-header ‖ pebble-key.
	storageAAD := encryption.AppendHeaderAADBytes(nil, encryption.EnvelopeVersionV1, 0, kid)
	storageAAD = append(storageAAD, []byte("synthetic 9B header")...)
	storageAAD = append(storageAAD, []byte("synthetic pebble key")...)
	body, err := c.Encrypt([]byte("payload"), storageAAD, kid, nonce)
	if err != nil {
		t.Fatalf("Encrypt storage-style: %v", err)
	}
	var nonceArr [encryption.NonceSize]byte
	copy(nonceArr[:], nonce)
	env := encryption.Envelope{
		Version: encryption.EnvelopeVersionV1,
		Flag:    0,
		KeyID:   kid,
		Nonce:   nonceArr,
		Body:    body,
	}
	storageEncoded, err := env.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Feed the storage envelope to the raft unwrap path.
	_, err = encryption.UnwrapRaftPayload(c, storageEncoded)
	if !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected ErrIntegrity for layer-confusion replay, got %v", err)
	}
}

// TestRaftEnvelope_RejectsRetiredKey confirms that an envelope
// whose key_id has been deleted from the keystore (DEK retirement
// or sidecar mismatch) surfaces ErrUnknownKeyID rather than a
// silent garbage decrypt.
func TestRaftEnvelope_RejectsRetiredKey(t *testing.T) {
	t.Parallel()
	ks, kid := newKeystoreWithKey(t)
	c := mustCipher(t, ks)
	encoded, err := encryption.WrapRaftPayload(c, kid, newRandomNonce(t), []byte("payload"))
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	ks.Delete(kid)
	_, err = encryption.UnwrapRaftPayload(c, encoded)
	if !errors.Is(err, encryption.ErrUnknownKeyID) {
		t.Fatalf("expected ErrUnknownKeyID after retire, got %v", err)
	}
}

func TestRaftEnvelope_RejectsStorageCompressionFlag(t *testing.T) {
	t.Parallel()
	c, keyID := raftFixture(t)
	nonce := newRandomNonce(t)
	payload := []byte("raft payload")
	aad := encryption.BuildRaftAAD(encryption.EnvelopeVersionV1, keyID)
	body, err := c.Encrypt(payload, aad, keyID, nonce)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	var nonceArray [encryption.NonceSize]byte
	copy(nonceArray[:], nonce)
	encoded, err := (&encryption.Envelope{
		Version: encryption.EnvelopeVersionV1,
		Flag:    encryption.FlagCompressed,
		KeyID:   keyID,
		Nonce:   nonceArray,
		Body:    body,
	}).Encode()
	if err != nil {
		t.Fatalf("Envelope.Encode: %v", err)
	}
	if _, err := encryption.UnwrapRaftPayload(c, encoded); !errors.Is(err, encryption.ErrEnvelopeFlag) {
		t.Fatalf("expected ErrEnvelopeFlag, got %v", err)
	}
}

// TestRaftEnvelope_ShortInputRejected covers DecodeEnvelope's
// length precondition (HeaderSize + TagSize = 34 bytes minimum).
func TestRaftEnvelope_ShortInputRejected(t *testing.T) {
	t.Parallel()
	c, _ := raftFixture(t)
	for _, l := range []int{0, 1, 17, 33} {
		_, err := encryption.UnwrapRaftPayload(c, make([]byte, l))
		if !errors.Is(err, encryption.ErrEnvelopeShort) {
			t.Fatalf("len=%d: expected ErrEnvelopeShort, got %v", l, err)
		}
	}
}

func TestRaftEnvelope_RejectsBadNonceSize(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	for _, n := range []int{0, 1, 11, 13} {
		_, err := encryption.WrapRaftPayload(c, kid, make([]byte, n), []byte("p"))
		if !errors.Is(err, encryption.ErrBadNonceSize) {
			t.Fatalf("nonce=%d: expected ErrBadNonceSize, got %v", n, err)
		}
	}
}

func TestRaftEnvelope_NilCipher(t *testing.T) {
	t.Parallel()
	if _, err := encryption.WrapRaftPayload(nil, 1, make([]byte, encryption.NonceSize), nil); !errors.Is(err, encryption.ErrNilKeystore) {
		t.Fatalf("Wrap: expected ErrNilKeystore, got %v", err)
	}
	if _, err := encryption.UnwrapRaftPayload(nil, make([]byte, encryption.EnvelopeOverhead)); !errors.Is(err, encryption.ErrNilKeystore) {
		t.Fatalf("Unwrap: expected ErrNilKeystore, got %v", err)
	}
}

func newRandomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

// TestBuildRaftAAD pins the byte layout: 'R' ‖ version ‖ key_id BE.
func TestBuildRaftAAD(t *testing.T) {
	t.Parallel()
	got := encryption.BuildRaftAAD(0x01, 0xCAFEBABE)
	want := []byte{'R', 0x01, 0xCA, 0xFE, 0xBA, 0xBE}
	if !bytes.Equal(got, want) {
		t.Fatalf("AAD mismatch:\n got  %x\n want %x", got, want)
	}
}

// TestRaftEnvelope_NoLeakedPlaintextBytes — defensive: an encoded
// envelope must NOT contain the raw plaintext as a suffix. A
// regression where the envelope appended plaintext alongside the
// ciphertext would leak data on disk; this catches any such bug.
func TestRaftEnvelope_NoLeakedPlaintextBytes(t *testing.T) {
	t.Parallel()
	c, kid := raftFixture(t)
	plaintext := newRandomBytes(t, 256)
	encoded, err := encryption.WrapRaftPayload(c, kid, newRandomNonce(t), plaintext)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if bytes.Contains(encoded, plaintext) {
		t.Fatal("encoded envelope contains the plaintext suffix verbatim")
	}
}
