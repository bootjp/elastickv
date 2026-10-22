// Package encryptiontest provides isolated encryption fixtures for tests.
package encryptiontest

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
)

var nonceFactoryID atomic.Uint64

const keyFileMode = 0o600

// NewCipher installs one fresh random DEK at keyID.
func NewCipher(tb testing.TB, keyID uint32) *encryption.Cipher {
	tb.Helper()
	key := make([]byte, encryption.KeySize)
	if _, err := rand.Read(key); err != nil {
		tb.Fatalf("generate DEK: %v", err)
	}
	ks := encryption.NewKeystore()
	if err := ks.Set(keyID, key); err != nil {
		tb.Fatalf("install DEK: %v", err)
	}
	cipher, err := encryption.NewCipher(ks)
	if err != nil {
		tb.Fatalf("create cipher: %v", err)
	}
	return cipher
}

// WriteSidecar uses the production writer in an isolated temporary directory.
func WriteSidecar(tb testing.TB, sc *encryption.Sidecar) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), encryption.SidecarFilename)
	if err := encryption.WriteSidecar(path, sc); err != nil {
		tb.Fatalf("write sidecar: %v", err)
	}
	return path
}

// NewNonceFactory gives each factory a distinct process-local fixture ID.
// This is test isolation, not the durable epoch protocol used by production.
func NewNonceFactory(tb testing.TB) *encryption.DeterministicNonceFactory {
	tb.Helper()
	id := nonceFactoryID.Add(1)
	if id > math.MaxUint32 {
		tb.Fatal("test nonce factory IDs exhausted")
	}
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], id)
	return encryption.NewDeterministicNonceFactory(binary.BigEndian.Uint16(encoded[4:6]), binary.BigEndian.Uint16(encoded[6:8]))
}

// WriteKEKFile writes a fresh raw AES-256 KEK readable only by its owner.
func WriteKEKFile(tb testing.TB) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "kek.bin")
	key := make([]byte, encryption.KeySize)
	if _, err := rand.Read(key); err != nil {
		tb.Fatalf("generate KEK: %v", err)
	}
	if err := os.WriteFile(path, key, keyFileMode); err != nil {
		tb.Fatalf("write KEK: %v", err)
	}
	return path
}
