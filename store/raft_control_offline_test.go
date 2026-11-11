package store

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestOfflinePlainControlMetadataUsesRuntimeValueCodec(t *testing.T) {
	t.Parallel()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	s, ok := st.(*pebbleStore)
	require.True(t, ok)
	key := []byte("J/offline-test")
	physical, err := raftControlKey(key)
	require.NoError(t, err)
	value := []byte("BAP1 trusted preparation")
	encoded, err := EncodeOfflinePlainControlMetadata(value)
	require.NoError(t, err)
	require.NoError(t, s.db.Set(physical, encoded, pebble.Sync))
	got, err := s.ReadRaftControlMetadata(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, value, got)
	require.ErrorIs(t, s.StageRaftControlMetadata(t.Context(), key, value), ErrRaftControlScopeRequired)
}

func TestOfflinePlainControlMetadataRejectsUnboundedBodies(t *testing.T) {
	t.Parallel()
	_, err := EncodeOfflinePlainControlMetadata(nil)
	require.Error(t, err)
	_, err = EncodeOfflinePlainControlMetadata(bytes.Repeat([]byte{1}, raftbootstrap.MaxFileSize+1))
	require.Error(t, err)
}
