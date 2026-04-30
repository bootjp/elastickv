package encryption_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
)

const testKeyID uint32 = 0xDEADBEEF

// newKeystoreWithKey returns a Keystore with one freshly-drawn DEK at
// testKeyID. Tests that need the raw DEK bytes can call ks.Get(testKeyID).
func newKeystoreWithKey(t *testing.T) (*encryption.Keystore, uint32) {
	t.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	if err := ks.Set(testKeyID, dek); err != nil {
		t.Fatalf("ks.Set: %v", err)
	}
	return ks, testKeyID
}

func newRandomNonce(t *testing.T) []byte {
	t.Helper()
	n := make([]byte, encryption.NonceSize)
	if _, err := rand.Read(n); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return n
}

func TestCipher_RoundTrip(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	cases := []struct {
		name      string
		plaintext []byte
		aad       []byte
	}{
		{"empty plaintext", []byte{}, []byte("aad")},
		{"empty aad", []byte("hello"), nil},
		{"both populated", []byte("hello world"), []byte("ctx")},
		{"binary plaintext", []byte{0x00, 0x01, 0xff, 0xfe, 0x7f, 0x80}, []byte("ctx")},
		{"large plaintext", bytes.Repeat([]byte("X"), 64*1024), []byte("ctx")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nonce := newRandomNonce(t)
			ct, err := c.Encrypt(tc.plaintext, tc.aad, keyID, nonce)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}
			if got, want := len(ct), len(tc.plaintext)+encryption.TagSize; got != want {
				t.Fatalf("ciphertext len = %d, want %d", got, want)
			}
			pt, err := c.Decrypt(ct, tc.aad, keyID, nonce)
			if err != nil {
				t.Fatalf("Decrypt: %v", err)
			}
			if !bytes.Equal(pt, tc.plaintext) {
				t.Fatalf("plaintext mismatch")
			}
		})
	}
}

func TestCipher_Decrypt_TagTamper(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	ct, err := c.Encrypt([]byte("payload"), []byte("aad"), keyID, nonce)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	// Flip the last byte (in the GCM tag region) and confirm Decrypt fails.
	ct[len(ct)-1] ^= 0x01
	_, err = c.Decrypt(ct, []byte("aad"), keyID, nonce)
	if err == nil || !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected ErrIntegrity, got %v", err)
	}
}

func TestCipher_Decrypt_AADTamper(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	ct, err := c.Encrypt([]byte("payload"), []byte("original aad"), keyID, nonce)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	_, err = c.Decrypt(ct, []byte("different aad"), keyID, nonce)
	if err == nil || !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected ErrIntegrity, got %v", err)
	}
}

func TestCipher_Decrypt_CiphertextTamper(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	ct, err := c.Encrypt([]byte("payload"), []byte("aad"), keyID, nonce)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	// Flip the first ciphertext byte (well before the tag).
	ct[0] ^= 0x01
	_, err = c.Decrypt(ct, []byte("aad"), keyID, nonce)
	if err == nil || !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected ErrIntegrity, got %v", err)
	}
}

func TestCipher_Decrypt_NonceTamper(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	ct, err := c.Encrypt([]byte("payload"), []byte("aad"), keyID, nonce)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	// Flip a bit in the nonce passed to Decrypt — this is a different
	// nonce, so GCM verification fails.
	nonce[0] ^= 0x01
	_, err = c.Decrypt(ct, []byte("aad"), keyID, nonce)
	if err == nil || !errors.Is(err, encryption.ErrIntegrity) {
		t.Fatalf("expected ErrIntegrity, got %v", err)
	}
}

func TestCipher_Encrypt_RejectsReservedKeyID(t *testing.T) {
	ks := encryption.NewKeystore()
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	_, err := c.Encrypt([]byte("x"), nil, encryption.ReservedKeyID, nonce)
	if !errors.Is(err, encryption.ErrReservedKeyID) {
		t.Fatalf("expected ErrReservedKeyID, got %v", err)
	}
}

func TestCipher_Encrypt_RejectsBadNonceSize(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	for _, nonceLen := range []int{0, 1, 11, 13, 16} {
		_, err := c.Encrypt([]byte("x"), nil, keyID, make([]byte, nonceLen))
		if !errors.Is(err, encryption.ErrBadNonceSize) {
			t.Fatalf("nonce=%d: expected ErrBadNonceSize, got %v", nonceLen, err)
		}
	}
}

func TestCipher_Encrypt_UnknownKeyID(t *testing.T) {
	ks := encryption.NewKeystore()
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	_, err := c.Encrypt([]byte("x"), nil, 12345, nonce)
	if !errors.Is(err, encryption.ErrUnknownKeyID) {
		t.Fatalf("expected ErrUnknownKeyID, got %v", err)
	}
}

func TestCipher_Decrypt_UnknownKeyID(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	nonce := newRandomNonce(t)
	ct, err := c.Encrypt([]byte("x"), nil, keyID, nonce)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	// Forget the DEK and try to decrypt.
	ks.Delete(keyID)
	_, err = c.Decrypt(ct, nil, keyID, nonce)
	if !errors.Is(err, encryption.ErrUnknownKeyID) {
		t.Fatalf("expected ErrUnknownKeyID, got %v", err)
	}
}

func TestCipher_DistinctNoncesProduceDistinctCiphertexts(t *testing.T) {
	ks, keyID := newKeystoreWithKey(t)
	c := encryption.NewCipher(ks)
	plaintext := []byte("same message")
	aad := []byte("same aad")
	n1 := newRandomNonce(t)
	n2 := newRandomNonce(t)
	if bytes.Equal(n1, n2) {
		t.Fatal("test bug: random nonces collided")
	}
	ct1, err := c.Encrypt(plaintext, aad, keyID, n1)
	if err != nil {
		t.Fatalf("Encrypt n1: %v", err)
	}
	ct2, err := c.Encrypt(plaintext, aad, keyID, n2)
	if err != nil {
		t.Fatalf("Encrypt n2: %v", err)
	}
	if bytes.Equal(ct1, ct2) {
		t.Fatal("ciphertexts collided despite different nonces")
	}
}
