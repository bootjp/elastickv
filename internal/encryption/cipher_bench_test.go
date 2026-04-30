package encryption_test

import (
	"crypto/rand"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

// benchPlaintextSizes covers the value-size range that storage / raft
// envelopes hit on the hot path: small (Redis counters, SET keys),
// medium (typical KV payloads, JSON), large (DynamoDB items, S3 small
// objects). 64 KiB caps the upper end so the benchmark stays under a
// few seconds; bigger plaintexts are AES-NI-bound and the per-byte
// throughput is already extracted at 64 KiB.
var benchPlaintextSizes = []int{64, 1024, 16 * 1024, 64 * 1024}

func setupBench(b *testing.B) (*encryption.Cipher, uint32, []byte) {
	b.Helper()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		b.Fatalf("rand.Read dek: %v", err)
	}
	if err := ks.Set(testKeyID, dek); err != nil {
		b.Fatalf("Set: %v", err)
	}
	c, err := encryption.NewCipher(ks)
	if err != nil {
		b.Fatalf("NewCipher: %v", err)
	}
	nonce := make([]byte, encryption.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		b.Fatalf("rand.Read nonce: %v", err)
	}
	return c, testKeyID, nonce
}

// BenchmarkCipher_Encrypt verifies the post-keystore-redesign hot path:
// Set installed the AEAD once, so each Encrypt call should be a single
// atomic.Pointer load + AEAD.Seal.
func BenchmarkCipher_Encrypt(b *testing.B) {
	c, keyID, nonce := setupBench(b)
	aad := []byte("storage-aad-context")

	for _, size := range benchPlaintextSizes {
		plaintext := make([]byte, size)
		if _, err := rand.Read(plaintext); err != nil {
			b.Fatalf("rand.Read plaintext: %v", err)
		}
		b.Run(name(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.Encrypt(plaintext, aad, keyID, nonce); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCipher_Decrypt mirrors BenchmarkCipher_Encrypt for the read
// side. Each iteration runs AEAD.Open against the same ciphertext so we
// measure the steady-state cost rather than seeding overhead.
func BenchmarkCipher_Decrypt(b *testing.B) {
	c, keyID, nonce := setupBench(b)
	aad := []byte("storage-aad-context")

	for _, size := range benchPlaintextSizes {
		plaintext := make([]byte, size)
		if _, err := rand.Read(plaintext); err != nil {
			b.Fatalf("rand.Read plaintext: %v", err)
		}
		ct, err := c.Encrypt(plaintext, aad, keyID, nonce)
		if err != nil {
			b.Fatalf("Encrypt: %v", err)
		}
		b.Run(name(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.Decrypt(ct, aad, keyID, nonce); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkKeystore_AEAD exercises the hot-path lookup in isolation.
// AEAD() should be a single atomic.Pointer load + map lookup; no
// allocations, no per-call AES key expansion.
func BenchmarkKeystore_AEAD(b *testing.B) {
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		b.Fatalf("rand.Read: %v", err)
	}
	if err := ks.Set(testKeyID, dek); err != nil {
		b.Fatalf("Set: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := ks.AEAD(testKeyID); !ok {
			b.Fatal("AEAD lookup missed")
		}
	}
}

// name returns a sub-benchmark label scaling with size for readable
// benchstat output ("64B", "1.0KiB", "16KiB", "64KiB").
func name(size int) string {
	switch {
	case size < 1024:
		return formatBytes(size, "B")
	case size < 1024*1024:
		return formatBytes(size/1024, "KiB")
	default:
		return formatBytes(size/(1024*1024), "MiB")
	}
}

func formatBytes(n int, unit string) string {
	// Avoid fmt.Sprintf to keep the benchmark file self-contained on
	// the smallest possible import surface.
	digits := []byte{}
	if n == 0 {
		digits = append(digits, '0')
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits) + unit
}
