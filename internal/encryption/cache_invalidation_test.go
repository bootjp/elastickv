package encryption

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageCacheInvalidatesBeforeKeyAndRegistrationChanges(t *testing.T) {
	c := NewStateCache()
	sc := &Sidecar{Active: ActiveKeys{Storage: 1}, StorageEnvelopeActive: true}
	calls := 0
	c.SetBeforeEncryptionChange(func() func() {
		calls++
		_, active := c.ActiveStorageKeyID()
		require.False(t, active)
		require.False(t, c.StorageEnvelopeActive())
		return func() { require.True(t, c.StorageEnvelopeActive()) }
	})
	c.RefreshFromSidecar(sc)
	require.Equal(t, 1, calls)
	c.SetBeforeEncryptionChange(func() func() {
		calls++
		require.False(t, c.Registered())
		return func() { require.True(t, c.Registered()) }
	})
	c.MarkRegistered(1)
	require.True(t, c.Registered())
	require.Equal(t, 2, calls)
	c.SetBeforeEncryptionChange(nil)
	c.MarkRegistered(2)
	require.False(t, c.Registered())
	require.Equal(t, 2, calls)
}
