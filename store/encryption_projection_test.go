package store

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

func TestEncryptionWriteObservationReadsInstalledPath(t *testing.T) {
	t.Parallel()
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(1, make([]byte, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	factory := encryption.NewDeterministicNonceFactory(123, 7)
	active, registered := true, true
	st, err := NewPebbleStore(filepath.Join(t.TempDir(), "db"),
		WithEncryption(cipher, factory, func() (uint32, bool) { return 1, true }),
		WithStorageEnvelopeGate(func() bool { return active }),
		WithStorageRegistrationGate(func() bool { return registered }))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	s, ok := st.(*pebbleStore)
	require.True(t, ok)
	got, err := s.ObserveEncryptionWritePath()
	require.NoError(t, err)
	require.Equal(t, EncryptionWriteObservation{KeyID: 1, NodeID: 123, Epoch: 7, KeyLoaded: true, EnvelopeActive: true, Registered: true}, got)
	nonce, err := factory.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(nonce[4:]))
	active, registered = false, false
	keys.Delete(1)
	got, err = s.ObserveEncryptionWritePath()
	require.NoError(t, err)
	require.False(t, got.KeyLoaded)
	require.False(t, got.EnvelopeActive)
	require.False(t, got.Registered)
}

func TestEncryptionWriteObservationRejectsUnwiredAndClosedStore(t *testing.T) {
	t.Parallel()
	st, err := NewPebbleStore(filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	s, ok := st.(*pebbleStore)
	require.True(t, ok)
	got, err := s.ObserveEncryptionWritePath()
	require.Error(t, err)
	require.Zero(t, got)
	require.NoError(t, st.Close())
	got, err = s.ObserveEncryptionWritePath()
	require.Error(t, err)
	require.Zero(t, got)
}
