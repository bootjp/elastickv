package encryption_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"pgregory.net/rapid"
)

// TestCipher_RoundTripProperty checks that for arbitrary plaintext, AAD,
// nonce, and key_id, Encrypt followed by Decrypt with the same inputs
// recovers the exact plaintext. CLAUDE.md flags this kind of test as the
// canonical safety net for envelope-format / compress-then-encrypt bugs.
func TestCipher_RoundTripProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ks := encryption.NewKeystore()
		// Random 32-byte DEK installed at a random non-reserved key_id.
		dek := make([]byte, encryption.KeySize)
		if _, err := rand.Read(dek); err != nil {
			t.Fatalf("rand.Read dek: %v", err)
		}
		keyID := rapid.Uint32Range(1, 0xFFFFFFFF).Draw(t, "keyID")
		if err := ks.Set(keyID, dek); err != nil {
			t.Fatalf("ks.Set: %v", err)
		}

		c := encryption.NewCipher(ks)
		plaintext := rapid.SliceOfN(rapid.Byte(), 0, 4096).Draw(t, "plaintext")
		aad := rapid.SliceOfN(rapid.Byte(), 0, 256).Draw(t, "aad")
		nonce := make([]byte, encryption.NonceSize)
		if _, err := rand.Read(nonce); err != nil {
			t.Fatalf("rand.Read nonce: %v", err)
		}

		ct, err := c.Encrypt(plaintext, aad, keyID, nonce)
		if err != nil {
			t.Fatalf("Encrypt: %v", err)
		}
		got, err := c.Decrypt(ct, aad, keyID, nonce)
		if err != nil {
			t.Fatalf("Decrypt: %v", err)
		}
		// Both empty plaintext and empty got should compare equal; treat
		// nil and empty as the same here so the property holds for the
		// 0-length boundary.
		if len(got) == 0 && len(plaintext) == 0 {
			return
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("plaintext mismatch:\n  got %x\n  want %x", got, plaintext)
		}
	})
}

// TestCipher_AADTamperProperty checks that any single-bit flip in the AAD
// at decrypt time causes ErrIntegrity. This is the property that backs the
// §4.1 cut-and-paste / blob-relocation defence.
func TestCipher_AADTamperProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ks := encryption.NewKeystore()
		dek := make([]byte, encryption.KeySize)
		if _, err := rand.Read(dek); err != nil {
			t.Fatalf("rand.Read dek: %v", err)
		}
		keyID := uint32(1)
		if err := ks.Set(keyID, dek); err != nil {
			t.Fatalf("ks.Set: %v", err)
		}
		c := encryption.NewCipher(ks)

		plaintext := rapid.SliceOfN(rapid.Byte(), 1, 256).Draw(t, "plaintext")
		// AAD must be at least 1 byte so we can tamper.
		aad := rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "aad")
		nonce := make([]byte, encryption.NonceSize)
		if _, err := rand.Read(nonce); err != nil {
			t.Fatalf("rand.Read nonce: %v", err)
		}

		ct, err := c.Encrypt(plaintext, aad, keyID, nonce)
		if err != nil {
			t.Fatalf("Encrypt: %v", err)
		}

		idx := rapid.IntRange(0, len(aad)-1).Draw(t, "idx")
		bit := rapid.Uint8Range(0, 7).Draw(t, "bit")
		bad := append([]byte(nil), aad...)
		bad[idx] ^= 1 << bit

		_, err = c.Decrypt(ct, bad, keyID, nonce)
		if !errors.Is(err, encryption.ErrIntegrity) {
			t.Fatalf("expected ErrIntegrity for tampered AAD, got %v", err)
		}
	})
}
