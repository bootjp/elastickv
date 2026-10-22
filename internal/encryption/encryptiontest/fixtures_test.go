package encryptiontest_test

import (
	"os"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/encryptiontest"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/stretchr/testify/require"
)

func TestCipherAndNonceFixtures(t *testing.T) {
	cipher := encryptiontest.NewCipher(t, 1)
	nonce, err := encryptiontest.NewNonceFactory(t).Next()
	require.NoError(t, err)
	encrypted, err := cipher.Encrypt([]byte("payload"), []byte("aad"), 1, nonce[:])
	require.NoError(t, err)
	plain, err := cipher.Decrypt(encrypted, []byte("aad"), 1, nonce[:])
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), plain)
	_, err = encryptiontest.NewCipher(t, 1).Decrypt(encrypted, []byte("aad"), 1, nonce[:])
	require.Error(t, err, "fixtures must not share DEKs")
}

func TestNonceFactoriesAreDisjointAndConcurrent(t *testing.T) {
	const count = 32
	values := make(chan [encryption.NonceSize]byte, count)
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() {
			nonce, err := encryptiontest.NewNonceFactory(t).Next()
			if err != nil {
				t.Error(err)
				return
			}
			values <- nonce
		})
	}
	wg.Wait()
	close(values)
	seen := make(map[[encryption.NonceSize]byte]bool)
	for nonce := range values {
		require.False(t, seen[nonce])
		seen[nonce] = true
	}
	require.Len(t, seen, count)
}

func TestSidecarAndKEKFile(t *testing.T) {
	var path string
	t.Run("owner", func(t *testing.T) {
		path = encryptiontest.WriteSidecar(t, &encryption.Sidecar{Keys: map[string]encryption.SidecarKey{}})
		sc, err := encryption.ReadSidecar(path)
		require.NoError(t, err)
		require.Equal(t, encryption.SidecarVersion, sc.Version)
		keyPath := encryptiontest.WriteKEKFile(t)
		stat, err := os.Stat(keyPath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), stat.Mode().Perm())
		wrapper, err := kek.NewFileWrapper(keyPath)
		require.NoError(t, err)
		key := make([]byte, encryption.KeySize)
		wrapped, err := wrapper.Wrap(key)
		require.NoError(t, err)
		plain, err := wrapper.Unwrap(wrapped)
		require.NoError(t, err)
		require.Equal(t, key, plain)
	})
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}
