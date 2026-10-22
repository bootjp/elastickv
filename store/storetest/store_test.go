package storetest_test

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption/encryptiontest"
	"github.com/bootjp/elastickv/store"
	"github.com/bootjp/elastickv/store/storetest"
	"github.com/stretchr/testify/require"
)

func TestNewPebbleOptionsAndCleanup(t *testing.T) {
	var st store.MVCCStore
	t.Run("owner", func(t *testing.T) {
		st = storetest.NewPebble(t, store.WithEncryption(
			encryptiontest.NewCipher(t, 1), encryptiontest.NewNonceFactory(t), func() (uint32, bool) { return 1, true },
		))
		require.NoError(t, st.PutAt(context.Background(), []byte("key"), []byte("value"), 1, 0))
		got, err := st.GetAt(context.Background(), []byte("key"), 1)
		require.NoError(t, err)
		require.Equal(t, []byte("value"), got)
	})
	require.Panics(t, func() { _, _ = st.GetAt(context.Background(), []byte("key"), 1) }, "Pebble rejects reads after cleanup")
}
